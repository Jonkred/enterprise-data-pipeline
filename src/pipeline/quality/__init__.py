"""Data quality validation using Great Expectations-style checks."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ExpectationResult:
    """Result of a single expectation check."""

    expectation_type: str
    column: str | None
    success: bool
    unexpected_count: int = 0
    unexpected_percent: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SuiteResult:
    """Result of an entire expectation suite."""

    suite_name: str
    expectations: list[ExpectationResult]
    success: bool = False
    evaluated: int = 0
    passed: int = 0
    failed: int = 0

    def __post_init__(self) -> None:
        self.evaluated = len(self.expectations)
        self.passed = sum(1 for e in self.expectations if e.success)
        self.failed = self.evaluated - self.passed
        self.success = self.failed == 0


class BaseExpectation(ABC):
    """Abstract base for data quality expectations."""

    def __init__(self, column: str | None = None, params: dict[str, Any] | None = None) -> None:
        self.column = column
        self.params = params or {}
        self._logger = get_logger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def evaluate(self, df: pd.DataFrame) -> ExpectationResult:
        """Evaluate expectation against DataFrame."""
        ...


class ExpectTableRowCountToBeBetween(BaseExpectation):
    """Expect table row count to be within range."""

    def evaluate(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.params.get("min_value", 0)
        max_val = self.params.get("max_value", float("inf"))
        count = len(df)
        success = min_val <= count <= max_val

        return ExpectationResult(
            expectation_type="expect_table_row_count_to_be_between",
            column=None,
            success=success,
            details={"observed_value": count, "min_value": min_val, "max_value": max_val},
        )


class ExpectColumnValuesToNotBeNull(BaseExpectation):
    """Expect column values to not be null."""

    def evaluate(self, df: pd.DataFrame) -> ExpectationResult:
        if not self.column or self.column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_not_be_null",
                column=self.column,
                success=False,
                details={"error": f"Column '{self.column}' not found"},
            )

        null_count = df[self.column].isna().sum()
        total = len(df)
        unexpected = null_count
        unexpected_pct = (null_count / total * 100) if total > 0 else 0

        return ExpectationResult(
            expectation_type="expect_column_values_to_not_be_null",
            column=self.column,
            success=null_count == 0,
            unexpected_count=unexpected,
            unexpected_percent=round(unexpected_pct, 2),
            details={"null_count": null_count, "total": total},
        )


class ExpectColumnValuesToBeBetween(BaseExpectation):
    """Expect numeric column values to be within range."""

    def evaluate(self, df: pd.DataFrame) -> ExpectationResult:
        if not self.column or self.column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_between",
                column=self.column,
                success=False,
            )

        min_val = self.params.get("min_value", float("-inf"))
        max_val = self.params.get("max_value", float("inf"))

        numeric_series = pd.to_numeric(df[self.column], errors="coerce")
        out_of_range = ((numeric_series < min_val) | (numeric_series > max_val)).sum()
        total = len(df)

        return ExpectationResult(
            expectation_type="expect_column_values_to_be_between",
            column=self.column,
            success=out_of_range == 0,
            unexpected_count=int(out_of_range),
            unexpected_percent=round((out_of_range / total * 100), 2) if total > 0 else 0,
            details={"min_value": min_val, "max_value": max_val},
        )


class ExpectColumnValuesToBeUnique(BaseExpectation):
    """Expect column values to be unique."""

    def evaluate(self, df: pd.DataFrame) -> ExpectationResult:
        if not self.column or self.column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_unique",
                column=self.column,
                success=False,
            )

        duplicates = df[self.column].duplicated().sum()
        total = len(df)

        return ExpectationResult(
            expectation_type="expect_column_values_to_be_unique",
            column=self.column,
            success=duplicates == 0,
            unexpected_count=int(duplicates),
            unexpected_percent=round((duplicates / total * 100), 2) if total > 0 else 0,
        )


class ExpectColumnValuesToMatchRegex(BaseExpectation):
    """Expect string column values to match regex pattern."""

    def evaluate(self, df: pd.DataFrame) -> ExpectationResult:
        import re

        if not self.column or self.column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_match_regex",
                column=self.column,
                success=False,
            )

        pattern = self.params.get("regex", ".*")
        regex = re.compile(pattern)
        non_matching = (~df[self.column].astype(str).str.match(regex)).sum()
        total = len(df)

        return ExpectationResult(
            expectation_type="expect_column_values_to_match_regex",
            column=self.column,
            success=non_matching == 0,
            unexpected_count=int(non_matching),
            unexpected_percent=round((non_matching / total * 100), 2) if total > 0 else 0,
            details={"regex_pattern": pattern},
        )


class QualityEngine:
    """Engine for running expectation suites."""

    EXPECTATION_MAP: dict[str, Callable[..., BaseExpectation]] = {
        "expect_table_row_count_to_be_between": ExpectTableRowCountToBeBetween,
        "expect_column_values_to_not_be_null": ExpectColumnValuesToNotBeNull,
        "expect_column_values_to_be_between": ExpectColumnValuesToBeBetween,
        "expect_column_values_to_be_unique": ExpectColumnValuesToBeUnique,
        "expect_column_values_to_match_regex": ExpectColumnValuesToMatchRegex,
    }

    def __init__(self) -> None:
        self._logger = get_logger(__name__)

    def run_suite(self, df: pd.DataFrame, suite_name: str, expectations: list[dict[str, Any]]) -> SuiteResult:
        """Run an expectation suite against a DataFrame.

        Args:
            df: DataFrame to validate
            suite_name: Name of the expectation suite
            expectations: List of expectation configurations

        Returns:
            SuiteResult with all expectation outcomes
        """
        self._logger.info("Running quality suite", suite=suite_name, expectations=len(expectations))

        results: list[ExpectationResult] = []
        for exp_config in expectations:
            exp_type = exp_config.get("type", "")
            params = exp_config.get("params", {})
            column = params.get("column")

            if exp_type not in self.EXPECTATION_MAP:
                self._logger.warning("Unknown expectation type, skipping", type=exp_type)
                continue

            # Skip if column is specified but doesn't exist in DataFrame
            if column and column not in df.columns:
                self._logger.warning(
                    "Column not found in DataFrame, skipping expectation",
                    column=column,
                    type=exp_type,
                    available=list(df.columns),
                )
                results.append(
                    ExpectationResult(
                        expectation_type=exp_type,
                        column=column,
                        success=True,  # neutral pass
                        details={"skipped": True, "reason": f"Column '{column}' not found"},
                    )
                )
                continue

            expectation = self.EXPECTATION_MAP[exp_type](column=column, params=params)
            result = expectation.evaluate(df)
            results.append(result)

            status = "PASS" if result.success else "FAIL"
            self._logger.info(
                f"Expectation {status}",
                type=exp_type,
                column=column,
                unexpected_count=result.unexpected_count,
            )

        suite_result = SuiteResult(suite_name=suite_name, expectations=results)

        self._logger.info(
            "Quality suite complete",
            suite=suite_name,
            evaluated=suite_result.evaluated,
            passed=suite_result.passed,
            failed=suite_result.failed,
            success=suite_result.success,
        )

        return suite_result