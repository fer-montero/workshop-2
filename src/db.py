"""Database connection helpers. Credentials come from environment variables only."""
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL


def get_engine(database: str):
    """Return a SQLAlchemy engine for the given PostgreSQL database."""
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
        host=os.environ.get("DATA_DB_HOST", "data-db"),
        port=int(os.environ.get("DATA_DB_PORT", "5432")),
        database=database,
    )
    return create_engine(url, pool_pre_ping=True)


def source_engine():
    return get_engine(os.environ.get("GRAMMY_SOURCE_DB", "grammy_source"))


def dw_engine():
    return get_engine(os.environ.get("MUSIC_DW_DB", "music_dw"))