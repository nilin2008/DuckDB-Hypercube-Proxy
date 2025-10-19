# 🦆 DuckDB Hypercube Proxy

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9%2B-blue.svg?logo=python" alt="Python 3.9+">
  <img src="https://img.shields.io/badge/FastAPI-🚀-green?logo=fastapi" alt="FastAPI">
  <img src="https://img.shields.io/badge/DuckDB-InMemory-orange?logo=duckdb" alt="DuckDB">
  <img src="https://img.shields.io/badge/PostgreSQL-asyncpg-blue?logo=postgresql" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/License-MIT-lightgrey.svg" alt="License: MIT">
  <br/>
  <em>High-speed analytical proxy between PostgreSQL and BI tools</em>
</p>

---
## 📘 Назначение

**DuckDB Hypercube Proxy** — промежуточный слой между BI-системой и PostgreSQL.  
Он позволяет кэшировать агрегированные запросы в памяти через **DuckDB**, выполняя роль быстрой “in-memory OLAP-прослойки”.

**Основные цели:**
- Разгрузка PostgreSQL при множественных BI-запросах  
- Мгновенная отдача повторных или вложенных агрегаций  
- Работа как REST/OData-совместимый сервис без изменений на стороне BI

---
| Компонент              | Назначение                                                    |
| ---------------------- | ------------------------------------------------------------- |
| **PgClient**           | Асинхронный клиент PostgreSQL (`asyncpg`)                     |
| **DuckEnv**            | In-memory DuckDB с ленивым сохранением (`persistent_enabled`) |
| **HypercubeCache**     | Хранение предагрегатов (`cache_*` таблицы) в оперативке       |
| **HypercubeManager**   | Управление SQL-запросами и кэш-механизмом                     |
| **CacheInvalidator**   | Проверяет `data_updates`, сбрасывает кэш при изменении        |
| **SQL-модуль**         | Нормализация и фильтрация SQL (`sqlglot`)                     |
| **persist_lazy()**     | Ленивое сохранение кэша в файл DuckDB                         |
| **metrics_endpoint()** | Сбор статистики кэша, hits/misses, persist                    |

⚙️ Алгоритмы кэширования:
- Первый запрос — выполняется в PostgreSQL, результат сохраняется в DuckDB.cache_*
- Повторный запрос — берётся из памяти (cache.has(...))
- Агрегация “вверх” — выполняется локально в DuckDB
- Invalidator — сравнивает значение из data_updates, при изменении сбрасывает кэш
- Persist — при завершении сохраняет все кэши в persistent_path, при старте — восстанавливает

🪣 Ленивое сохранение кэша:
- Активируется при duckdb.persistent_enabled = true
- Файл базы задаётся через duckdb.persistent_path
- При завершении работы — кэш записывается в файл
- При запуске — таблицы cache_* восстанавливаются из него

⚡ Особенности:
- Разрешены только SELECT-запросы
- Все операции выполняются в оперативной памяти
- Поддерживается ленивое сохранение в persistent_path
- Кэш автоматически восстанавливается при старте
- Инвалидатор проверяет таблицу обновлений и сбрасывает кэш при изменениях
- /metrics и /ping для мониторинга

---
## ⚙️ Требования

- **Python** ≥ 3.9  
- **Docker** (для локального теста PostgreSQL)  
- **RAM:** 4 ГБ + (рекомендуется ≥ 8 ГБ)  
- **OS:** Windows / Linux / macOS  

## 📦 Установка и запуск
1️⃣ Создание окружения
```bash
python -m venv venv
venv\Scripts\activate        # Windows
# или
source venv/bin/activate     # Linux/macOS

pip install -r requirements.txt
```
2️⃣ Конфигурация (config.yaml)
```yaml
server:
  host: "0.0.0.0"
  port: 8000

postgres:
  dsn: "postgresql://user:pass@localhost:5432/postgres"
  pool_min: 2
  pool_max: 8
  statement_timeout_ms: 600000

duckdb:
  memory_limit: "8GB"
  threads: 8
  cache_ttl_seconds: 900
  persistent_enabled: true
  persistent_path: "cache.duckdb"

cache:
  max_tables: 2000
  max_rows_per_table: 5000000

invalidator:
  enabled: true
  check_interval_seconds: 60
  query: "SELECT COALESCE(MAX(updated_at)::text, '0') FROM public.data_updates"

security:
  allow_raw_select_only: true
  forbid_semicolons: true

logging:
  sql_log_top_k: 100

hypercube:
  allowed_dimensions: ["p1","p2","p3"]
  measure: "count"
  source_table: "public.facts_agg"
  base_where: ""
  max_dimensionality: 3
```
3️⃣ Тестовый прогон
```bash
python test_proxy.py
```
Скрипт автоматически:
- запускает PostgreSQL в Docker;
- создаёт таблицу public.facts_agg;
- наполняет 1 000 000 строк;
- запускает FastAPI-прокси (app.py);
- проверяет скорость кэширования и повторных запросов.

Пример вывода:
```log
🐘 PostgreSQL готов.
📦 Подготовка тестовых данных...
🚀 FastAPI-прокси запущен.
Первый запрос (build cache): 0.36 с
Повторный запрос (cache hit): 0.02 с
Агрегация "вверх": 0.07 с
✅ Тест завершён успешно.
```
| Метод    | URL          | Назначение                                             | Формат             |
| -------- | ------------ | ------------------------------------------------------ | ------------------ |
| **POST** | `/query`     | SQL-запрос (только SELECT), работает через кэш         | `text/csv`         |
| **GET**  | `/cube`      | REST-куб: `dims=p1,p2&metrics=SUM(count)&filters=p3>5` | `text/csv`         |
| **GET**  | `/cube.json` | То же, но в JSON (для REST/OData)                      | `application/json` |
| **GET**  | `/ping`      | Проверка жизни и размера кэша                          | `application/json` |
| **GET**  | `/metrics`   | Статистика: кэш, persist, hits/misses                  | `application/json` |

Примеры запросов:

🔹 SQL-запрос
```bash
curl -X POST "http://localhost:8000/query" ^
  -H "Content-Type: application/json" ^
  -d "{\"sql\": \"SELECT p1,p2,SUM(count) FROM public.facts_agg GROUP BY p1,p2\"}"
```
🔹 REST-куб
```bash
curl "http://localhost:8000/cube?dims=p1,p2&metrics=SUM(count)&filters=p3>5" -o result.csv
```
🔹 Метрики
```bash
curl http://localhost:8000/metrics
```
Пример ответа:
```json
{
  "tables_cached": 3,
  "total_rows": 150000,
  "cache_keys": [["p1","p2"],["p1"]],
  "cache_hits": 12,
  "cache_misses": 4,
  "last_persist_ts": 1739864000.12
}
```
