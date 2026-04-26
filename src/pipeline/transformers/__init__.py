"""Data transformation layer with Pandas-based operations."""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for data transformers."""

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        self.name = name
        self.config = config or {}
        self._logger = get_logger(f"{__name__}.{name}")

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation to input DataFrame."""
        ...

    def get_metadata(self) -> dict[str, Any]:
        """Get transformation metadata."""
        return {"transformer": self.name, "config": self.config}


class DeduplicationTransformer(BaseTransformer):
    """Remove duplicates keeping most recent record."""

    def __init__(
        self,
        key_columns: list[str],
        timestamp_column: str = "updated_at",
        strategy: str = "merge",
    ) -> None:
        super().__init__("deduplication", {"key_columns": key_columns, "strategy": strategy})
        self.key_columns = key_columns
        self.timestamp_column = timestamp_column
        self.strategy = strategy

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate records by key, keeping latest."""
        if df.empty:
            return df

        # Filter to columns that actually exist in the DataFrame
        available_keys = [c for c in self.key_columns if c in df.columns]
        if not available_keys:
            self._logger.warning(
                "No key columns found in DataFrame, skipping deduplication",
                requested=self.key_columns,
                available=list(df.columns),
            )
            return df

        initial_count = len(df)

        if self.timestamp_column in df.columns:
            df = df.sort_values(self.timestamp_column)

        df = df.drop_duplicates(subset=available_keys, keep="last")

        final_count = len(df)
        removed = initial_count - final_count

        self._logger.info(
            "Deduplication complete",
            initial_rows=initial_count,
            final_rows=final_count,
            removed=removed,
            keys_used=available_keys,
        )
        return df


class EnrichmentTransformer(BaseTransformer):
    """Enrich data by joining with reference dimensions."""

    def __init__(self, lookup_df: pd.DataFrame, join_keys: list[str]) -> None:
        super().__init__("enrichment", {"join_keys": join_keys})
        self.lookup_df = lookup_df
        self.join_keys = join_keys

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Join with lookup data."""
        if df.empty:
            return df

        available_keys = [c for c in self.join_keys if c in df.columns]
        if not available_keys:
            self._logger.warning(
                "No join keys found in DataFrame, skipping enrichment",
                requested=self.join_keys,
                available=list(df.columns),
            )
            return df

        initial_cols = len(df.columns)
        result = df.merge(self.lookup_df, on=available_keys, how="left", suffixes=("", "_lookup"))
        new_cols = len(result.columns) - initial_cols

        self._logger.info(
            "Enrichment complete",
            rows=len(result),
            new_columns=new_cols,
            join_keys=available_keys,
        )
        return result


class AggregationTransformer(BaseTransformer):
    """Aggregate metrics by grouping dimensions."""

    def __init__(self, group_by: list[str], metrics: list[str]) -> None:
        super().__init__("aggregation", {"group_by": group_by, "metrics": metrics})
        self.group_by = group_by
        self.metrics = metrics

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Group and aggregate data."""
        if df.empty:
            return df

        # Filter group_by to columns that actually exist
        available_groups = [c for c in self.group_by if c in df.columns]
        if not available_groups:
            self._logger.warning(
                "No group-by columns found in DataFrame, skipping aggregation",
                requested=self.group_by,
                available=list(df.columns),
            )
            return df

        if len(available_groups) < len(self.group_by):
            self._logger.info(
                "Some group-by columns not found, using subset",
                requested=self.group_by,
                available=available_groups,
            )

        # Parse metric expressions like "sum(revenue) as total_revenue"
        agg_dict: dict[str, Any] = {}
        col_renames: dict[str, str] = {}

        for metric in self.metrics:
            parts = metric.split(" as ")
            expr = parts[0].strip()
            alias = parts[1].strip() if len(parts) > 1 else expr

            # Extract column name from function(col)
            col = None
            if "(" in expr and ")" in expr:
                col = expr[expr.find("(") + 1:expr.rfind(")")]

            if not col or col not in df.columns:
                self._logger.warning("Metric column not found, skipping", metric=metric, column=col)
                continue

            if expr.startswith("sum("):
                agg_dict[col] = "sum"
                col_renames[f"sum({col})"] = alias
            elif expr.startswith("count("):
                agg_dict[col] = "count"
                col_renames[f"count({col})"] = alias
            elif expr.startswith("avg(") or expr.startswith("mean("):
                agg_dict[col] = "mean"
                col_renames[f"mean({col})"] = alias
            elif expr.startswith("min("):
                agg_dict[col] = "min"
                col_renames[f"min({col})"] = alias
            elif expr.startswith("max("):
                agg_dict[col] = "max"
                col_renames[f"max({col})"] = alias

        if not agg_dict:
            self._logger.warning("No valid metrics to aggregate, returning original data")
            return df

        result = df.groupby(available_groups).agg(agg_dict).reset_index()
        result.columns = [col_renames.get(str(c), str(c)) for c in result.columns]

        self._logger.info(
            "Aggregation complete",
            groups=len(result),
            dimensions=len(available_groups),
            metrics=len(agg_dict),
        )
        return result


class TypeCastTransformer(BaseTransformer):
    """Cast columns to specified types with safe conversions."""

    def __init__(self, type_map: dict[str, str]) -> None:
        super().__init__("type_cast", type_map)
        self.type_map = type_map

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns to specified types."""
        if df.empty:
            return df

        for col, dtype in self.type_map.items():
            if col not in df.columns:
                self._logger.warning("Column not found for type casting, skipping", column=col)
                continue

            try:
                if dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "string":
                    df[col] = df[col].astype(str)
                else:
                    df[col] = df[col].astype(dtype)
            except Exception:
                self._logger.error("Type cast failed, skipping column", column=col, target_dtype=dtype)
                continue

        self._logger.info("Type casting complete", columns=list(self.type_map.keys()))
        return df


class PipelineTransformer:
    """Orchestrates multiple transformations in sequence."""

    def __init__(self) -> None:
        self.transformers: list[BaseTransformer] = []
        self._logger = get_logger(__name__)

    def add(self, transformer: BaseTransformer) -> "PipelineTransformer":
        """Add a transformer to the pipeline."""
        self.transformers.append(transformer)
        return self

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute all transformations in sequence."""
        self._logger.info(
            "Starting transformation pipeline",
            steps=len(self.transformers),
            input_rows=len(df),
        )

        result = df
        for transformer in self.transformers:
            self._logger.info("Running transformer", name=transformer.name)
            result = transformer.transform(result)

        self._logger.info(
            "Transformation pipeline complete",
            output_rows=len(result),
            columns=len(result.columns),
        )
        return result