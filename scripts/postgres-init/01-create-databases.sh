#!/bin/sh
set -eu

# POSTGRES_DB is already created by the image entrypoint.
# We only need to provision the Airflow metadata database.
if [ "$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -tAc "SELECT 1 FROM pg_database WHERE datname='airflow'")" != "1" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c "CREATE DATABASE airflow;"
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c "GRANT ALL PRIVILEGES ON DATABASE warehouse TO $POSTGRES_USER;"
