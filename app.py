from __future__ import annotations
import asyncio, os, io, csv, re, time, json, hashlib
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
import time
import docker
import duckdb
import asyncpg
import pandas as pd
import sqlglot
from sqlglot import expressions as sx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi import Request
from pydantic import BaseModel
import yaml
from contextlib import asynccontextmanager
import argparse
import uvicorn
from fastapi.responses import JSONResponse

# =========================================================
# CONFIG
# =========================================================

def load_config() -> dict:
    cfg = {}
    if os.path.exists("config.yaml"):
        with open("config.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

    cfg.setdefault("postgres", {})["dsn"] = cfg["postgres"].get("dsn", "postgresql://user:pass@localhost/postgres")
    dcli = docker.from_env()
    for cont in dcli.containers.list():
        if cont.name == 'pg_hypercube_test':
            ports = cont.ports
            mapped = ports.get(f"5432/tcp")
            if mapped and isinstance(mapped, list) and mapped[0].get("HostPort"):
                host_port = mapped[0]["HostPort"]
                cfg.setdefault("postgres", {})["dsn"] =  cfg["postgres"].get("dsn").replace("5432", host_port)

    cfg.setdefault("duckdb", {})["memory_limit"] = "8GB"
    cfg.setdefault("duckdb", {})["threads"] = 8
    return cfg

CFG = load_config()

# =========================================================
# INVALIDATOR TASK
# =========================================================

class CacheInvalidator:
    def __init__(self, manager: HypercubeManager, cfg: dict):
        self.manager = manager
        inv_cfg = cfg.get("invalidator", {})
        self.enabled = inv_cfg.get("enabled", False)
        self.query = inv_cfg.get("query", "")
        self.interval = inv_cfg.get("check_interval_seconds", 60)
        self._task: Optional[asyncio.Task] = None
        self._last_value: Optional[str] = None
        self.pg = manager.pg

    async def start(self):
        if not self.enabled or not self.query:
            print("⚙️  Инвалидатор выключен в конфиге.")
            return
        print(f"🧩 Запуск инвалидатора (каждые {self.interval}s)")
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while True:
            try:
                async with self.pg.pool.acquire() as conn:
                    val = await conn.fetchval(self.query)
                if self._last_value and val != self._last_value:
                    print("♻️  Обнаружено обновление данных, сбрасываю кэш.")
                    self.manager.cache.db.execute("DROP ALL TABLES")
                    self.manager.cache.meta.clear()
                self._last_value = val
            except Exception as e:
                print(f"⚠️  Ошибка в инвалидаторе: {e}")
            await asyncio.sleep(self.interval)

# =========================================================
# SQL UTILS
# =========================================================

SELECT_ONLY_RE = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)

def is_safe_sql(sql: str) -> bool:
    if ";" in sql.strip().rstrip(";"):
        return False
    if not SELECT_ONLY_RE.match(sql):
        return False
    lower = sql.lower()
    for kw in ["insert","update","delete","alter","drop","truncate","merge"]:
        if kw in lower:
            return False
    return True

def normalize_sql(sql: str) -> str:
    try:
        expr = sqlglot.parse_one(sql)
        return expr.sql(dialect="postgres").strip()
    except Exception:
        return re.sub(r"\s+", " ", sql.strip())

def rewrite_sql(sql: str) -> str:
    """Упрощает SQL (убирает дубли, сортирует поля, чистит alias)"""
    try:
        expr = sqlglot.parse_one(sql)
        if isinstance(expr.this, sx.Subquery):
            expr = expr.this.this or expr.this
        if expr.args.get("where"):
            w = str(expr.args["where"]).strip().lower()
            if w in ("true", "where true"):
                expr.set("where", None)
        if expr.args.get("group"):
            seen = set()
            new_g = []
            for g in expr.args["group"].expressions:
                if g.sql() not in seen:
                    new_g.append(g)
                    seen.add(g.sql())
            expr.set("group", sx.Group(expressions=sorted(new_g, key=lambda e: e.sql())))
        selects = []
        for s in expr.selects:
            if s.alias_or_name and s.name == s.alias_or_name:
                s.set("alias", None)
            selects.append(s)
        expr.set("expressions", sorted(selects, key=lambda e: e.sql()))
        return re.sub(r"\s+", " ", expr.sql(dialect="postgres").strip())
    except Exception:
        return sql

# =========================================================
# POSTGRES CLIENT
# =========================================================

class PgClient:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def start(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=2, max_size=8)

    async def fetch_df(self, sql: str) -> pd.DataFrame:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
        if not rows:
            return pd.DataFrame()
        cols = list(rows[0].keys())
        data = [tuple(r.values()) for r in rows]
        return pd.DataFrame.from_records(data, columns=cols)

# =========================================================
# HYPERCUBE CACHE / MANAGER
# =========================================================

@dataclass
class CacheEntry:
    fields: Tuple[str, ...]
    created: float
    last_hit: float
    ttl: float

class HypercubeCache:
    def __init__(self, duck: duckdb.DuckDBPyConnection, ttl: int = 900):
        self.db = duck
        self.meta: Dict[str, CacheEntry] = {}
        self.ttl = ttl
        # метрики
        self.hits: int = 0
        self.misses: int = 0

    def _key(self, fields: Tuple[str, ...]) -> str:
        return "cache_" + "_".join(fields)

    def has(self, fields: Tuple[str, ...]) -> bool:
        k = self._key(fields)
        e = self.meta.get(k)
        if not e:
            self.misses += 1
            return False
        if time.time() - e.created > e.ttl:
            self.db.execute(f"DROP TABLE IF EXISTS {k}")
            del self.meta[k]
            self.misses += 1
            return False
        self.hits += 1
        return True

    def create(self, fields: Tuple[str, ...], df: pd.DataFrame):
        k = self._key(fields)
        self.db.execute(f"CREATE TABLE IF NOT EXISTS {k} AS SELECT * FROM df")
        self.meta[k] = CacheEntry(fields, time.time(), time.time(), self.ttl)

    def get(self, fields: Tuple[str, ...]) -> pd.DataFrame:
        k = self._key(fields)
        df = self.db.execute(f"SELECT * FROM {k}").df()
        self.meta[k].last_hit = time.time()
        return df

class HypercubeManager:
    def __init__(self, pg: PgClient, duckdb_conn: duckdb.DuckDBPyConnection, ttl: int = 900):
        self.pg = pg
        self.duck = duckdb_conn
        self.cache = HypercubeCache(duckdb_conn, ttl=ttl)

    async def query(self, sql: str) -> pd.DataFrame:
        t0 = time.time()
        expr = sqlglot.parse_one(sql)
        group = expr.args.get("group")
        if not group:
            result = await self.pg.fetch_df(sql)
            print(f"Fetch DF за: {time.time() - t0:.3f} с")
            return result
        group_fields = tuple(sorted([g.sql() for g in group.expressions]))
        if self.cache.has(group_fields):
            result = self.cache.get(group_fields)
            print(f"Из кэша: {group_fields} за: {time.time() - t0:.3f} с")
            return result
        df = await self.pg.fetch_df(sql)
        self.cache.create(group_fields, df)
        print(f"Из базы: {sql} за: {time.time() - t0:.3f} с")
        return df

# =========================================================
# DUCKDB ENV (with optional persistent cache)
# =========================================================

class DuckEnv:
    def __init__(self, cfg: dict):
        duck_cfg = cfg.get("duckdb", {})
        self.persistent_enabled: bool = duck_cfg.get("persistent_enabled", False)
        self.persistent_path: Optional[str] = duck_cfg.get("persistent_path", None)

        # ВАЖНО: всегда работаем в памяти
        self.db = duckdb.connect(database=":memory:")
        self.db.execute(f"PRAGMA memory_limit='{duck_cfg['memory_limit']}';")
        self.db.execute(f"PRAGMA threads={int(duck_cfg['threads'])};")

        # служебное поле: время последнего persist
        self.last_persist_ts: Optional[float] = None

    def _attach_persist_db(self) -> bool:
        """
        Подключает (ATTACH) файл-базу для persist по self.persistent_path.
        Возвращает True если получилось подключить, иначе False.
        """
        if not (self.persistent_enabled and self.persistent_path):
            return False
        # создаём файл при необходимости (duckdb сам создаст при ATTACH)
        try:
            self.db.execute(f"ATTACH DATABASE '{self.persistent_path}' AS persist;")
            return True
        except Exception as e:
            print(f"⚠️ ATTACH persist DB failed: {e}")
            return False

    def _detach_persist_db(self):
        try:
            self.db.execute("DETACH DATABASE persist;")
        except Exception:
            pass

    def load_persistent(self, cache: 'HypercubeCache', ttl: int):
        """
        При старте: читает из persist-базы все таблицы cache_* и восстанавливает их в память,
        а также заполняет cache.meta (ключи, ttl).
        """
        if not (self.persistent_enabled and self.persistent_path):
            return
        if not os.path.exists(self.persistent_path):
            return

        if not self._attach_persist_db():
            return

        try:
            # список таблиц в persist
            tbls = self.db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND database_name='persist'").fetchall()
            names = [t[0] for t in tbls if t and t[0].startswith("cache_")]
            restored = 0
            for name in names:
                # копируем в память таблицу с тем же именем
                self.db.execute(f"CREATE TABLE {name} AS SELECT * FROM persist.{name}")
                # восстанавливаем метаданные cache.meta
                try:
                    # имя: cache_p1_p2_... -> tuple('p1','p2',...)
                    fields = tuple(name.split("cache_", 1)[1].split("_"))
                    cache.meta[name] = CacheEntry(fields=fields, created=time.time(), last_hit=time.time(), ttl=ttl)
                    restored += 1
                except Exception:
                    # если имя необычное — пропускаем
                    self.db.execute(f"DROP TABLE IF EXISTS {name}")
            if restored:
                print(f"💾 Восстановлено из persist: {restored} кэш-таблиц")
        finally:
            self._detach_persist_db()

    async def persist_lazy(self, cache: 'HypercubeCache'):
        """
        Перед завершением работы (или по расписанию): лениво сохраняет cache_* таблицы
        в файл-базу, не влияя на основной in-memory режим.
        """
        if not (self.persistent_enabled and self.persistent_path):
            return
        if not cache.meta:
            return

        if not self._attach_persist_db():
            return

        try:
            for name in list(cache.meta.keys()):
                # создаём/заменяем таблицу в persist-базе
                self.db.execute(f"CREATE OR REPLACE TABLE persist.{name} AS SELECT * FROM {name}")
            self.last_persist_ts = time.time()
            print(f"💾 Ленивое сохранение кэша: {len(cache.meta)} таблиц → {self.persistent_path}")
        finally:
            self._detach_persist_db()

# =========================================================
# FASTAPI SERVER (modern lifespan)
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    dsn = os.getenv("PG_DSN", CFG["postgres"]["dsn"])
    print(f"📡 Подключение к PostgreSQL: {dsn}")
    pg = PgClient(dsn)
    await pg.start()

    duck_env = DuckEnv(CFG)
    manager = HypercubeManager(pg, duck_env.db, ttl=CFG["duckdb"].get("cache_ttl_seconds", 900))

    # 🔹 восстанавливаем кэш из persist-файла, если включено
    try:
        duck_env.load_persistent(manager.cache, ttl=CFG["duckdb"].get("cache_ttl_seconds", 900))
    except Exception as e:
        print(f"⚠️ Ошибка при восстановлении persist-кэша: {e}")

    invalidator = CacheInvalidator(manager, CFG)
    await invalidator.start()

    app.state.manager = manager
    app.state.duck_env = duck_env
    app.state.invalidator = invalidator

    yield

    # 🔹 ленивое сохранение кэша перед завершением
    try:
        await duck_env.persist_lazy(manager.cache)
    except Exception as e:
        print(f"⚠️ Ошибка при сохранении persist-кэша: {e}")

    if pg.pool:
        await pg.pool.close()

app = FastAPI(title="DuckDB Hypercube Proxy", version="3.1", lifespan=lifespan)

class SqlRequest(BaseModel):
    sql: str

def csv_stream(df: pd.DataFrame) -> StreamingResponse:
    def gen():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(df.columns)
        yield buf.getvalue().encode("utf-8-sig")  # BOM для Excel
        buf.seek(0); buf.truncate(0)
        for row in df.itertuples(index=False, name=None):
            writer.writerow(row)
            yield buf.getvalue().encode("utf-8")
            buf.seek(0); buf.truncate(0)

    headers = {
        "Content-Disposition": 'attachment; filename="result.csv"',
        "Cache-Control": "no-store",
        "Pragma": "no-cache"
    }
    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers=headers
    )


@app.post("/query", response_class=StreamingResponse)
async def query_endpoint(req: SqlRequest, request: Request):
    sql = req.sql.strip()
    if not is_safe_sql(sql):
        raise HTTPException(400, "Only SELECT allowed")

    norm = normalize_sql(sql)
    rewritten = rewrite_sql(norm)
    manager: HypercubeManager = request.app.state.manager
    try:
        df = await manager.query(rewritten)
    except Exception as e:
        raise HTTPException(500, f"Execution error: {e}")
    return csv_stream(df)

@app.get("/cube", response_class=StreamingResponse)
async def cube_endpoint( #GET /cube?dims=p1,p2&metrics=SUM(count)&filters=p3>5
    request: Request,
    dims: str,
    metrics: str = "SUM(count)",
    filters: Optional[str] = None
):
    """Ручка формирует SQL и пускает через тот же пайплайн"""
    dims_list = [d.strip() for d in dims.split(",") if d.strip()]
    where_clause = f"WHERE {filters}" if filters else ""
    sql = f"SELECT {','.join(dims_list)},{metrics} FROM {CFG['hypercube']['source_table']} {where_clause} GROUP BY {','.join(dims_list)}"
    manager: HypercubeManager = request.app.state.manager
    try:
        df = await manager.query(sql)
    except Exception as e:
        raise HTTPException(500, f"Execution error: {e}")
    return csv_stream(df)

@app.get("/cube.json")
async def cube_json_endpoint(
    request: Request,
    dims: str,
    metrics: str = "SUM(count)",
    filters: Optional[str] = None
):
    dims_list = [d.strip() for d in dims.split(",") if d.strip()]
    where_clause = f"WHERE {filters}" if filters else ""
    sql = f"SELECT {','.join(dims_list)},{metrics} FROM {CFG['hypercube']['source_table']} {where_clause} GROUP BY {','.join(dims_list)}"
    manager: HypercubeManager = request.app.state.manager
    df = await manager.query(sql)
    return JSONResponse(content=df.to_dict(orient="records"))

async def push_to_bi(target_url: str, df: pd.DataFrame, fmt: str = "csv"):
    import aiohttp
    async with aiohttp.ClientSession() as s:
        if fmt == "csv":
            data = df.to_csv(index=False)
            headers = {"Content-Type": "text/csv; charset=utf-8"}
            await s.post(target_url, data=data, headers=headers)
        else:
            data = df.to_json(orient="records", force_ascii=False)
            headers = {"Content-Type": "application/json; charset=utf-8"}
            await s.post(target_url, data=data, headers=headers)


@app.get("/ping")
async def ping():
    return {"status": "ok", "cache_size": len(app.state.manager.cache.meta)}

@app.get("/metrics")
async def metrics_endpoint(request: Request):
    cache = request.app.state.manager.cache
    duck_env: DuckEnv = request.app.state.duck_env
    stats = {
        "tables_cached": len(cache.meta),
        "total_rows": sum(
            cache.db.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            for tbl in [f"cache_{'_'.join(v.fields)}" for v in cache.meta.values()]
        ) if cache.meta else 0,
        "cache_keys": [list(v.fields) for v in cache.meta.values()],
        # новые метрики
        "cache_hits": cache.hits,
        "cache_misses": cache.misses,
        "last_persist_ts": getattr(duck_env, "last_persist_ts", None),
        "persistent_enabled": bool(getattr(duck_env, "persistent_enabled", False)),
        "persistent_path": getattr(duck_env, "persistent_path", None),
    }
    return stats

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run("app:app", host="0.0.0.0", port=args.port)
