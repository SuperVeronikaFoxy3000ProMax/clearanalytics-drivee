"""App and DB settings."""
from sqlalchemy import create_engine

DB_HOST = "localhost"
DB_PORT = 3306
DB_NAME = "drivee"

DATA_TABLES: tuple[str, ...] = ("incity", "pass_detail", "driver_detail")
PRIMARY_FACT_TABLE = "incity"

RO_USER = "analyst_ro"
RO_PASSWORD = ""

ADMIN_USER = "root"
ADMIN_PASSWORD = ""

QUERY_TIMEOUT_SEC = 90
FORCED_LIMIT = 1000

# LM Studio exposes OpenAI-compatible API on localhost:1234 by default.
# Модель: defog_-_llama-3-sqlcoder-8b.
LLM_BASE_URL = "http://localhost:1234/v1"
LLM_MODEL = "qwen/qwen3-coder-30b"
LLM_TIMEOUT_SEC = 120
LLM_MAX_TOKENS = 600
LLM_TEMPERATURE = 0.0


def _url(user: str, password: str, db: str | None = DB_NAME) -> str:
    db_part = f"/{db}" if db else ""
    return f"mysql+pymysql://{user}:{password}@{DB_HOST}:{DB_PORT}{db_part}"


engine_ro = create_engine(
    _url(RO_USER, RO_PASSWORD),
    pool_pre_ping=True,
    connect_args={
        "connect_timeout": 5,
        "read_timeout": QUERY_TIMEOUT_SEC,
        "init_command": f"SET SESSION max_statement_time={QUERY_TIMEOUT_SEC}",
    },
)

engine_admin = create_engine(
    _url(ADMIN_USER, ADMIN_PASSWORD),
    pool_pre_ping=True,
)
