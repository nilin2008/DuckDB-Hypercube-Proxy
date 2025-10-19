import asyncio
import docker
import time
import asyncpg
import aiohttp
import subprocess
import os
import sys

PG_IMAGE = "postgres:16"
PG_USER = "user"
PG_PASS = "pass"
PG_DB = "postgres"
PG_PORT = 5432
CONTAINER_NAME = "pg_hypercube_test"
VOLUME_NAME = "pg_hypercube_data"

if sys.platform.startswith("win"):
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def wait_pg_ready(timeout=180):
    """Ждёт, пока PostgreSQL готов принимать подключения."""
    print("⏳ Ожидание готовности PostgreSQL...")
    for _ in range(timeout):
        try:
            dsn = os.environ["PG_DSN"]
            conn = await asyncpg.connect(dsn)
            await conn.close()
            print("✅ PostgreSQL готов.")
            return True
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError("❌ PostgreSQL не готов после ожидания.")

async def prepare_pg():
    print("📦 Подготовка тестовых данных...")
    dsn = os.environ["PG_DSN"]
    conn = await asyncpg.connect(dsn)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.facts_agg (
            p1 INT,
            p2 INT,
            p3 INT,
            count INT,
            updated_at TIMESTAMP DEFAULT now()
        );
    """)
    exists = await conn.fetchval("SELECT COUNT(*) FROM public.facts_agg;")
    if exists == 0:
        print("➡️ Вставка 100k строк чанками по 10k...")
        rows = [(i % 100, i % 50, i % 10, 1) for i in range(100_000)]
        chunk_size = 10_000
        for i in range(0, len(rows), chunk_size):
            #await conn.copy_records_to_table(
            #    "facts_agg",
            #    records=rows[i:i+chunk_size]
            #)
            await conn.executemany("INSERT INTO facts_agg (p1,p2,p3,count) VALUES ($1,$2,$3,$4)", rows)
        print("✅ Вставка завершена.")
    else:
        print(f"ℹ️ Таблица уже содержит {exists} строк, пропускаем.")
    await conn.close()


async def run_test(port):
    print(f"🚀 Запуск FastAPI-прокси на порту {port}...")
    proc = subprocess.Popen([sys.executable, "app.py", "--port", str(port)])
    await asyncio.sleep(4)

    base_url = f"http://localhost:{port}"
    async with aiohttp.ClientSession() as s:
        # 1. Первый запрос — создаёт кэш
        t0 = time.time()
        q1 = "SELECT p1,p2,SUM(count) FROM public.facts_agg GROUP BY p1,p2"
        async with s.post(f"{base_url}/query", json={"sql": q1}) as r:
            await r.text()
        t1 = time.time()
        print(f"Первый запрос (build cache): {t1 - t0:.3f} с")

        # 2. Повторный запрос — hit
        t0 = time.time()
        async with s.post(f"{base_url}/query", json={"sql": q1}) as r:
            await r.text()
        t1 = time.time()
        print(f"Повторный запрос (cache hit): {t1 - t0:.3f} с")

        # 3. Агрегация вверх
        q2 = "SELECT p1,SUM(count) FROM public.facts_agg GROUP BY p1"
        t0 = time.time()
        async with s.post(f"{base_url}/query", json={"sql": q2}) as r:
            await r.text()
        t1 = time.time()
        print(f"Агрегация 'вверх' из пред-агрегата: {t1 - t0:.3f} с")

    proc.terminate()
    proc.wait()


async def main():
    dcli = docker.from_env()

    print("🧹 Проверка старых контейнеров...")
    try:
        old = dcli.containers.get(CONTAINER_NAME)
        print("Удаляю старый контейнер PostgreSQL...")
        old.remove(force=True)
        time.sleep(2)
    except docker.errors.NotFound:
        pass

    # создаём volume, если нет
    if not any(v.name == VOLUME_NAME for v in dcli.volumes.list()):
        print(f"💾 Создаю volume {VOLUME_NAME} (персистентные данные Postgres)...")
        dcli.volumes.create(name=VOLUME_NAME)

    print("🐘 Запуск контейнера PostgreSQL...")
    cont = dcli.containers.run(
        PG_IMAGE,
        name=CONTAINER_NAME,
        detach=True,
        ports={f"{PG_PORT}/tcp": None},
        environment={
            "POSTGRES_USER": PG_USER,
            "POSTGRES_PASSWORD": PG_PASS,
            "POSTGRES_DB": PG_DB,
        },
        volumes={VOLUME_NAME: {'bind': '/var/lib/postgresql/data', 'mode': 'rw'}},
    )

    # ждем пока Docker присвоит порт
    for i in range(20):
        cont.reload()
        ports = cont.attrs["NetworkSettings"]["Ports"]
        mapped = ports.get(f"{PG_PORT}/tcp")
        if mapped and isinstance(mapped, list) and mapped[0].get("HostPort"):
            host_port = mapped[0]["HostPort"]
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("❌ Docker не успел пробросить порт 5432/tcp")

    print(f"🔌 PostgreSQL проброшен на порт {host_port} хоста")

    # сохраняем DSN
    dsn = f"postgresql://{PG_USER}:{PG_PASS}@localhost:{host_port}/{PG_DB}"
    print(f"{dsn}")
    os.environ["PG_DSN"] = dsn

    # ждём готовности
    await wait_pg_ready()

    # проверяем, нужно ли перезапускать после initdb
    logs = cont.logs(tail=20).decode(errors="ignore")
    if "initdb" in logs or "database system is ready" not in logs:
        print("🔁 Похоже, это первый запуск — перезапускаем после инициализации...")
        cont.restart()
        await wait_pg_ready()

    await prepare_pg()
    await run_test(8000)

    print("\n✅ Тест завершён успешно. Контейнер остановлен, volume сохранён.")
    cont.stop()
    # не удаляем volume — база сохранится для будущих прогонов

if __name__ == "__main__":
    asyncio.run(main())
