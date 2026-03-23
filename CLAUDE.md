# CLAUDE.md — aurora-sync

Python daemon que sincroniza Aurora MySQL 8 (read-only) → MySQL 8 local en /Volumes/nvme.
Expone API REST en puerto 8090. El web UI vive en el repo `aurora-sync-ui` (Laravel 12).

## Stack

- Python 3.11, FastAPI, uvicorn
- PyMySQL para Aurora y MySQL local
- SQLite (WAL mode) para estado persistente en `/data/aurora_sync.db`
- Docker: mysql:8.0 (puerto 3308) + app (puerto 8090)
- MySQL data en `${MYSQL_DATA_PATH:-/Volumes/nvme/aurora-sync-mysql}`

## Estructura

```
app/
├── main.py              # FastAPI app + lifespan (Twingate monitor start/stop)
├── config.py            # Settings via pydantic-settings + .env
├── database.py          # SQLite init, WAL mode, schema migrations
├── aurora/
│   ├── connection.py    # PyMySQL connection a Aurora
│   ├── discovery.py     # Listar tablas, tamaños, columnas generadas, estrategia cursor
│   └── twingate.py      # TCP check loop + auto-pause en fallo
├── local/
│   └── connection.py    # PyMySQL local + ensure_database_exists()
├── sync/
│   ├── engine.py        # Lógica central: schema, data, views, SPs
│   ├── worker.py        # threading.Thread + pause/cancel events
│   └── batch.py         # bulk_replace() y bulk_upsert()
├── state/
│   └── repository.py    # Todo acceso a SQLite (thread-safe con _lock)
├── notifications/
│   └── telegram.py      # httpx POST a Telegram API
└── routes/
    ├── tables.py        # GET/POST /api/tables, discovery, selection
    ├── jobs.py          # CRUD jobs + pause/resume/cancel
    ├── progress.py      # GET /api/progress (polling)
    └── settings.py      # Settings + test-aurora/local/telegram
```

## Estrategias de cursor

| Estrategia | Condición | Comportamiento |
|---|---|---|
| `auto_increment` | PK con AUTO_INCREMENT | WHERE id > last_id (cursor eficiente) |
| `datetime` | Columna updated_at/modified_at | WHERE updated_at >= last_dt |
| `full` | Sin ninguna de las anteriores | LIMIT/OFFSET, TRUNCATE en inicio |

## Columnas generadas

`discovery.py` detecta columnas con `EXTRA LIKE '%generated%'`.
`engine.py` las excluye de SELECT e INSERT — MySQL las recalcula solo.
El CREATE TABLE se importa completo (con la expresión generada).

## Twingate

TCP connect a `AURORA_HOST:AURORA_PORT` cada `TWINGATE_CHECK_INTERVAL` segundos.
Si falla: pausa el job activo + notifica Telegram.
Historial en tabla `twingate_checks`.

## API base URL

Servidor: `http://192.168.1.113:8090`
