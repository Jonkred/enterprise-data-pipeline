"""Data loaders for multiple destination systems."""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.config import get_settings
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class BaseLoader(ABC):
    """Abstract base class for data loaders."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        self.name = name
        self.config = config or {}
        self._logger = get_logger(f"{__name__}.{name}")

    @abstractmethod
    def load(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame into destination. Returns rows written."""
        ...

    @abstractmethod
    def validate_destination(self) -> bool:
        """Validate destination is accessible."""
        ...


class SQLLoader(BaseLoader):
    """Load data into SQL databases with upsert support."""

    def __init__(
        self,
        connection_string: str | None = None,
        schema: str = "analytics",
        batch_size: int = 5000,
        write_mode: str = "upsert",
    ) -> None:
        super().__init__("sql", {"schema": schema, "write_mode": write_mode})
        self.connection_string = connection_string or get_settings().warehouse_connection_string
        self.batch_size = batch_size
        self.write_mode = write_mode
        self._engine = create_engine(self.connection_string)
        # SQLite does not support schemas; disable when detected
        self._is_sqlite = self._engine.dialect.name == "sqlite"
        self.schema = None if self._is_sqlite else schema

    def validate_destination(self) -> bool:
        """Test database connection."""
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            self._logger.warning("Database connection failed")
            return False

    def load(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame into SQL table."""
        if df.empty:
            self._logger.warning("Empty DataFrame, skipping load")
            return 0

        full_table_name = f"{self.schema}.{table_name}" if self.schema else table_name

        self._logger.info(
            "Loading data to warehouse",
            table=table_name if self._is_sqlite else f"{self.schema}.{table_name}",
            rows=len(df),
            mode=self.write_mode,
            dialect=self._engine.dialect.name,
        )

        # Create schema if not exists
        self._create_schema_if_not_exists()

        if self.write_mode == "replace":
            df.to_sql(
                table_name,
                self._engine,
                schema=self.schema,
                if_exists="replace",
                index=False,
                chunksize=self.batch_size,
            )
        elif self.write_mode == "append":
            df.to_sql(
                table_name,
                self._engine,
                schema=self.schema,
                if_exists="append",
                index=False,
                chunksize=self.batch_size,
            )
        elif self.write_mode == "upsert":
            self._upsert(df, table_name)
        else:
            raise ValueError(f"Unknown write mode: {self.write_mode}")

        self._logger.info("Load complete", table=full_table_name, rows=len(df))
        return len(df)

    def _create_schema_if_not_exists(self) -> None:
        """Create target schema if it doesn't exist (PostgreSQL only)."""
        if self._is_sqlite or not self.schema:
            return

        with self._engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))

    def _upsert(self, df: pd.DataFrame, table_name: str) -> None:
        """Perform upsert (merge) operation."""
        # Simplified upsert using pandas
        # In production, use SQL MERGE or ON CONFLICT for atomic upserts
        from sqlalchemy import inspect as sa_inspect

        inspector = sa_inspect(self._engine)
        table_exists = inspector.has_table(table_name, schema=self.schema)

        if not table_exists:
            self._logger.info("Table does not exist, creating with insert", table=table_name)
            df.to_sql(
                table_name,
                self._engine,
                schema=self.schema,
                if_exists="fail",  # fail is safe since we checked above
                index=False,
                chunksize=self.batch_size,
            )
            return

        existing = pd.read_sql_table(table_name, self._engine, schema=self.schema)

        if existing.empty:
            df.to_sql(
                table_name,
                self._engine,
                schema=self.schema,
                if_exists="append",
                index=False,
                chunksize=self.batch_size,
            )
        else:
            # Align columns + normalize dtypes before merge.
            # This avoids pandas concat failures when datetime precision differs
            # between SQL-read data (often us) and in-memory data (often ns).
            incoming = df.copy()
            all_columns = list(dict.fromkeys([*existing.columns.tolist(), *incoming.columns.tolist()]))

            for col in all_columns:
                if col not in existing.columns:
                    existing[col] = pd.NA
                if col not in incoming.columns:
                    incoming[col] = pd.NA

            existing = existing[all_columns].reset_index(drop=True)
            incoming = incoming[all_columns].reset_index(drop=True)

            datetime_cols: list[str] = []
            for col in all_columns:
                existing_dtype = str(existing[col].dtype)
                incoming_dtype = str(incoming[col].dtype)
                if "datetime64" in existing_dtype or "datetime64" in incoming_dtype:
                    datetime_cols.append(col)

            for col in datetime_cols:
                existing[col] = pd.to_datetime(existing[col], errors="coerce")
                incoming[col] = pd.to_datetime(incoming[col], errors="coerce")

            # pandas.concat can fail in some environments with mixed block internals
            # (for example, repeated datetime blocks with different backing units).
            # Building via records is slower but robust for this project scale.
            combined_records = existing.to_dict(orient="records")
            combined_records.extend(incoming.to_dict(orient="records"))
            combined = pd.DataFrame.from_records(combined_records, columns=all_columns)

            key_columns = [c for c in combined.columns if c.endswith("_id")]
            dedup_subset = key_columns if key_columns else None
            combined = combined.drop_duplicates(subset=dedup_subset, keep="last").reset_index(drop=True)
            combined.to_sql(
                table_name,
                self._engine,
                schema=self.schema,
                if_exists="replace",
                index=False,
                chunksize=self.batch_size,
            )


class ParquetLoader(BaseLoader):
    """Load data as Parquet files to local or cloud storage."""

    def __init__(
        self,
        base_path: str = "data/datalake",
        partition_cols: list[str] | None = None,
        compression: str = "snappy",
    ) -> None:
        super().__init__("parquet", {"compression": compression})
        self.base_path = base_path
        self.partition_cols = partition_cols or []
        self.compression = compression

    def validate_destination(self) -> bool:
        """Check if destination path is writable."""
        import os

        try:
            os.makedirs(self.base_path, exist_ok=True)
            return os.access(self.base_path, os.W_OK)
        except Exception:
            return False

    def load(self, df: pd.DataFrame, table_name: str) -> int:
        """Write DataFrame as partitioned Parquet."""
        import os

        if df.empty:
            return 0

        path = os.path.join(self.base_path, table_name)
        os.makedirs(path, exist_ok=True)

        self._logger.info(
            "Writing Parquet files",
            path=path,
            rows=len(df),
            partitions=self.partition_cols,
        )

        if self.partition_cols and all(c in df.columns for c in self.partition_cols):
            df.to_parquet(
                path,
                partition_cols=self.partition_cols,
                compression=self.compression,
                index=False,
            )
        else:
            output_file = os.path.join(path, f"{table_name}.parquet")
            df.to_parquet(output_file, compression=self.compression, index=False)

        self._logger.info("Parquet write complete", path=path, rows=len(df))
        return len(df)


def create_loader(loader_type: str, config: dict[str, Any]) -> BaseLoader:
    """Factory for creating loaders.

    Args:
        loader_type: Type of loader (sql, parquet, s3)
        config: Loader configuration

    Returns:
        Configured BaseLoader instance
    """
    if loader_type in ("sql", "postgres", "postgresql", "snowflake", "mysql"):
        return SQLLoader(
            connection_string=config.get("connection_string"),
            schema=config.get("schema", "analytics"),
            batch_size=config.get("batch_size", 5000),
            write_mode=config.get("write_mode", "upsert"),
        )
    elif loader_type in ("parquet", "s3"):
        # For S3, use local path simulation based on bucket name
        # In production, integrate with boto3/s3fs for real S3 writes
        if loader_type == "s3":
            bucket = config.get("bucket", "datalake")
            base_path = config.get("path", f"data/datalake/{bucket}")
        else:
            base_path = config.get("path", "data/datalake")
        return ParquetLoader(
            base_path=base_path,
            partition_cols=config.get("partition_by"),
            compression=config.get("compression", "snappy"),
        )
    else:
        raise ValueError(f"Unknown loader type: {loader_type}")
