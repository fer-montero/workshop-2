"""Environment verification: versions and database connectivity."""
import sys

sys.path.insert(0, "/opt/airflow")

import airflow
import great_expectations as gx
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import text

from src.db import dw_engine, source_engine

print("Python:", sys.version.split()[0])
print("Airflow:", airflow.__version__)
print("pandas:", pd.__version__)
print("Great Expectations:", gx.__version__)
print("SQLAlchemy:", sqlalchemy.__version__)
print("psycopg2:", psycopg2.__version__)

for name, engine in [("grammy_source", source_engine()), ("music_dw", dw_engine())]:
    with engine.connect() as conn:
        db = conn.execute(text("SELECT current_database()")).scalar()
        version = conn.execute(text("SHOW server_version")).scalar()
        print(f"Connection OK -> {name}: current_database()={db}, PostgreSQL {version}")