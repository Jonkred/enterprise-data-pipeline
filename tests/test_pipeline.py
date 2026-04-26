"""Tests for pipeline components."""

import os
from pathlib import Path

import pandas as pd
import pytest

from pipeline.config import get_settings
from pipeline.extractors import FileExtractor, ExtractorConfig
from pipeline.loaders import SQLLoader
from pipeline.quality import QualityEngine
from pipeline.transformers import (
    DeduplicationTransformer,
    PipelineTransformer,
    TypeCastTransformer,
)


class TestFileExtractor:
    """Test suite for file-based extraction."""

    def test_extract_parquet(self, tmp_path: Path) -> None:
        """Test extracting from Parquet files."""
        # Create test data
        df = pd.DataFrame({
            "id": range(1000),
            "value": [f"val_{i}" for i in range(1000)],
        })
        df.to_parquet(tmp_path / "test.parquet")

        config = ExtractorConfig(name="test", source_type="file")
        extractor = FileExtractor(config, str(tmp_path), "parquet", "*.parquet")

        result = extractor.extract()
        assert len(result) == 1000
        assert "_source_file" in result.columns
        assert "_extracted_at" in result.columns

    def test_empty_directory(self, tmp_path: Path) -> None:
        """Test extraction from empty directory returns empty DataFrame."""
        config = ExtractorConfig(name="test", source_type="file")
        extractor = FileExtractor(config, str(tmp_path), "parquet", "*.parquet")

        result = extractor.extract()
        assert result.empty

    def test_validate_connection(self, tmp_path: Path) -> None:
        """Test connection validation."""
        config = ExtractorConfig(name="test", source_type="file")
        extractor = FileExtractor(config, str(tmp_path), "parquet")

        assert extractor.validate_connection() is True

        nonexistent = FileExtractor(config, "/nonexistent/path", "parquet")
        assert nonexistent.validate_connection() is False


class TestTransformers:
    """Test suite for data transformations."""

    def test_deduplication(self) -> None:
        """Test deduplication transformer."""
        df = pd.DataFrame({
            "id": [1, 1, 2, 2, 3],
            "value": ["a", "b", "c", "d", "e"],
            "updated_at": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03", "2024-01-01"],
        })

        tx = DeduplicationTransformer(key_columns=["id"], timestamp_column="updated_at")
        result = tx.transform(df)

        assert len(result) == 3
        assert result[result["id"] == 1]["value"].iloc[0] == "b"  # Latest kept

    def test_type_cast(self) -> None:
        """Test type casting transformer."""
        df = pd.DataFrame({
            "id": ["1", "2", "3"],
            "amount": ["10.5", "20.3", "30.1"],
        })

        tx = TypeCastTransformer({"id": "int", "amount": "float"})
        result = tx.transform(df)

        assert result["id"].dtype == "int64"
        assert result["amount"].dtype == "float64"

    def test_pipeline_transformer(self) -> None:
        """Test sequential transformation pipeline."""
        df = pd.DataFrame({
            "id": [1, 1, 2],
            "value": ["a", "b", "c"],
            "updated_at": ["2024-01-01", "2024-01-02", "2024-01-01"],
        })

        pipeline = PipelineTransformer()
        pipeline.add(DeduplicationTransformer(key_columns=["id"]))

        result = pipeline.run(df)
        assert len(result) == 2


class TestQualityEngine:
    """Test suite for data quality validations."""

    def test_row_count_expectation(self) -> None:
        """Test table row count validation."""
        df = pd.DataFrame({"id": range(100)})
        engine = QualityEngine()

        result = engine.run_suite(
            df,
            "test_suite",
            [{"type": "expect_table_row_count_to_be_between", "params": {"min_value": 50, "max_value": 200}}],
        )

        assert result.success is True
        assert result.passed == 1

    def test_null_check(self) -> None:
        """Test null value validation."""
        df = pd.DataFrame({
            "id": [1, 2, None, 4],
            "name": ["a", "b", "c", "d"],
        })
        engine = QualityEngine()

        result = engine.run_suite(
            df,
            "test_suite",
            [{"type": "expect_column_values_to_not_be_null", "params": {"column": "id"}}],
        )

        assert result.success is False
        assert result.failed == 1

    def test_range_validation(self) -> None:
        """Test numeric range validation."""
        df = pd.DataFrame({"amount": [10, 50, 100, 200]})
        engine = QualityEngine()

        result = engine.run_suite(
            df,
            "test_suite",
            [{"type": "expect_column_values_to_be_between", "params": {"column": "amount", "min_value": 0, "max_value": 150}}],
        )

        assert result.success is False  # 200 is out of range
        assert result.expectations[0].unexpected_count == 1

    def test_unique_constraint(self) -> None:
        """Test uniqueness validation."""
        df = pd.DataFrame({"id": [1, 2, 3, 3]})
        engine = QualityEngine()

        result = engine.run_suite(
            df,
            "test_suite",
            [{"type": "expect_column_values_to_be_unique", "params": {"column": "id"}}],
        )

        assert result.success is False
        assert result.expectations[0].unexpected_count == 1

    def test_regex_pattern(self) -> None:
        """Test regex pattern validation."""
        df = pd.DataFrame({
            "email": ["valid@test.com", "invalid", "also@valid.com"],
        })
        engine = QualityEngine()

        result = engine.run_suite(
            df,
            "test_suite",
            [{"type": "expect_column_values_to_match_regex", "params": {"column": "email", "regex": r"^[\w\.-]+@[\w\.-]+\.\w{2,}$"}}],
        )

        assert result.success is False
        assert result.expectations[0].unexpected_count == 1


class TestSQLLoader:
    """Test suite for SQL loading."""

    def test_load_sqlite(self, tmp_path: Path) -> None:
        """Test loading data to SQLite."""
        db_path = tmp_path / "test.db"
        loader = SQLLoader(
            connection_string=f"sqlite:///{db_path}",
            schema=None,
            write_mode="replace",
        )

        df = pd.DataFrame({
            "id": range(1000),
            "value": [f"val_{i}" for i in range(1000)],
        })

        rows = loader.load(df, "test_table")
        assert rows == 1000

        # Verify data was written
        result = pd.read_sql_table("test_table", loader._engine)
        assert len(result) == 1000

    def test_empty_dataframe(self, tmp_path: Path) -> None:
        """Test loading empty DataFrame."""
        db_path = tmp_path / "test.db"
        loader = SQLLoader(
            connection_string=f"sqlite:///{db_path}",
            schema=None,
        )

        df = pd.DataFrame()
        rows = loader.load(df, "empty_table")
        assert rows == 0


class TestConfig:
    """Test suite for application configuration."""

    def test_settings_singleton(self) -> None:
        """Test settings are cached."""
        from pipeline.config import get_settings

        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2

    def test_development_mode(self) -> None:
        """Test development environment detection."""
        from pipeline.config import get_settings

        settings = get_settings()
        assert settings.is_development is True
        assert settings.is_production is False

    def test_connection_string(self) -> None:
        """Test SQLite connection string in dev mode."""
        from pipeline.config import get_settings

        settings = get_settings()
        conn = settings.warehouse_connection_string
        assert conn.startswith("sqlite:///")
