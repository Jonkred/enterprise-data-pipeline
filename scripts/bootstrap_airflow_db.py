"""Bootstrap required PostgreSQL databases for Airflow and warehouse."""

import os

import psycopg2


def ensure_databases() -> None:
    """Create required databases when they do not exist."""
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname="postgres",
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        for database in ("airflow", "warehouse"):
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if cur.fetchone() is None:
                cur.execute(f"CREATE DATABASE {database}")

    conn.close()


if __name__ == "__main__":
    ensure_databases()
