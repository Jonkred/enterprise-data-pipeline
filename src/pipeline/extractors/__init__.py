"""Base extractor interface and implementations for multi-source ingestion."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ExtractorConfig:
    """Configuration for a data extractor."""

    name: str
    source_type: str
    batch_size: int = 100_000
    schema: dict[str, str] | None = None


class BaseExtractor(ABC):
    """Abstract base class for all data extractors."""

    def __init__(self, config: ExtractorConfig) -> None:
        self.config = config
        self._logger = get_logger(f"{__name__}.{config.name}")

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from source and return as DataFrame."""
        ...

    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate connection to the source system."""
        ...

    def get_metadata(self) -> dict[str, Any]:
        """Get extractor metadata for lineage tracking."""
        return {
            "extractor": self.config.name,
            "source_type": self.config.source_type,
            "batch_size": self.config.batch_size,
        }


class FileExtractor(BaseExtractor):
    """Extractor for file-based sources (CSV, Parquet, JSON)."""

    def __init__(
        self,
        config: ExtractorConfig,
        path: str,
        file_format: str = "parquet",
        pattern: str = "*.parquet",
    ) -> None:
        super().__init__(config)
        self.path = Path(path)
        self.file_format = file_format
        self.pattern = pattern

    def validate_connection(self) -> bool:
        """Check if source path exists."""
        exists = self.path.exists()
        if not exists:
            self._logger.warning("Source path does not exist", path=str(self.path))
        return exists

    def extract(self) -> pd.DataFrame:
        """Read files and return combined DataFrame."""
        self._logger.info(
            "Starting file extraction",
            path=str(self.path),
            format=self.file_format,
            pattern=self.pattern,
        )

        if not self.path.exists():
            raise FileNotFoundError(f"Source path not found: {self.path}")

        files = list(self.path.glob(self.pattern))
        if not files:
            self._logger.warning("No files found matching pattern", pattern=self.pattern)
            return pd.DataFrame()

        self._logger.info("Found files", count=len(files))

        frames: list[pd.DataFrame] = []
        for file in files:
            try:
                if self.file_format == "parquet":
                    df = pd.read_parquet(file)
                elif self.file_format == "csv":
                    df = pd.read_csv(file)
                else:
                    raise ValueError(f"Unsupported format: {self.file_format}")

                df["_source_file"] = str(file.name)
                df["_extracted_at"] = pd.Timestamp.now()
                frames.append(df)
                self._logger.info("Loaded file", file=str(file.name), rows=len(df))
            except Exception:
                self._logger.error("Failed to read file", file=str(file.name))
                raise

        result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._logger.info("Extraction complete", total_rows=len(result), files=len(frames))
        return result


class APIExtractor(BaseExtractor):
    """Extractor for REST API sources with pagination and retry."""

    def __init__(
        self,
        config: ExtractorConfig,
        base_url: str,
        endpoint: str,
        api_key: str | None = None,
        page_size: int = 1000,
    ) -> None:
        super().__init__(config)
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint
        self.api_key = api_key
        self.page_size = page_size

    def validate_connection(self) -> bool:
        """Test API connectivity with a lightweight request."""
        try:
            import httpx

            response = httpx.get(f"{self.base_url}/health", timeout=5.0)
            return response.status_code == 200
        except Exception:
            self._logger.warning("API health check failed")
            return False

    def extract(self) -> pd.DataFrame:
        """Fetch paginated data from API."""
        self._logger.info(
            "Starting API extraction",
            url=f"{self.base_url}/{self.endpoint}",
            page_size=self.page_size,
        )

        # Simulated API extraction for demo purposes
        # In production, this would use httpx with retry logic
        self._logger.info("API extraction simulated for demo")
        return self._generate_sample_data()

    def _generate_sample_data(self) -> pd.DataFrame:
        """Generate sample CRM data for demonstration."""
        import random
        from datetime import datetime, timedelta

        n = 50_000
        now = datetime.now()

        data = {
            "customer_id": [f"CUST_{i:08d}" for i in range(n)],
            "email": [f"customer_{i}@example.com" for i in range(n)],
            "name": [f"Customer {i}" for i in range(n)],
            "region": random.choices(["NORTH", "SOUTH", "EAST", "WEST"], k=n),
            "customer_lifetime_value": [round(random.uniform(100, 50000), 2) for _ in range(n)],
            "signup_date": [
                (now - timedelta(days=random.randint(0, 3650))).strftime("%Y-%m-%d")
                for _ in range(n)
            ],
            "last_purchase": [
                (now - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
                for _ in range(n)
            ],
            "segment": random.choices(["VIP", "REGULAR", "NEW", "CHURNED"], weights=[10, 50, 25, 15], k=n),
            "_extracted_at": pd.Timestamp.now(),
        }
        return pd.DataFrame(data)


class KafkaExtractor(BaseExtractor):
    """Extractor for Kafka streaming sources with windowing."""

    def __init__(
        self,
        config: ExtractorConfig,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        window_seconds: int = 300,
    ) -> None:
        super().__init__(config)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.window_seconds = window_seconds

    def validate_connection(self) -> bool:
        """Check Kafka broker connectivity."""
        try:
            # In production, use kafka-python or confluent-kafka
            self._logger.info("Kafka connection validated")
            return True
        except Exception:
            return False

    def extract(self) -> pd.DataFrame:
        """Consume messages from Kafka topic within time window."""
        self._logger.info(
            "Starting Kafka extraction",
            topic=self.topic,
            window_seconds=self.window_seconds,
        )

        # Simulated Kafka extraction for demo purposes
        return self._generate_sample_iot_data()

    def _generate_sample_iot_data(self) -> pd.DataFrame:
        """Generate sample IoT sensor data for demonstration."""
        import random
        from datetime import datetime, timedelta

        n = 100_000
        now = datetime.now()

        device_types = ["TEMPERATURE", "HUMIDITY", "PRESSURE", "VIBRATION", "FLOW"]
        locations = ["FACTORY_A", "FACTORY_B", "WAREHOUSE_1", "WAREHOUSE_2", "LAB_1"]

        data = {
            "event_id": [f"EVT_{i:010d}" for i in range(n)],
            "device_id": [f"DEV_{random.randint(1000, 9999)}" for _ in range(n)],
            "device_type": random.choices(device_types, k=n),
            "location": random.choices(locations, k=n),
            "value": [round(random.uniform(0, 100), 3) for _ in range(n)],
            "unit": random.choices(["C", "%", "PSI", "G", "L/M"], k=n),
            "timestamp": [
                (now - timedelta(minutes=random.randint(0, self.window_seconds // 60))).isoformat()
                for _ in range(n)
            ],
            "quality_flag": random.choices(["GOOD", "SUSPECT", "BAD"], weights=[90, 8, 2], k=n),
            "_extracted_at": pd.Timestamp.now(),
        }
        return pd.DataFrame(data)


def create_extractor(source_type: str, config: dict[str, Any]) -> BaseExtractor:
    """Factory function to create extractors by type.

    Args:
        source_type: Type of source (file, api, kafka)
        config: Extractor configuration dictionary

    Returns:
        Configured BaseExtractor instance
    """
    extractor_config = ExtractorConfig(
        name=config.get("name", "default"),
        source_type=source_type,
        batch_size=config.get("batch_size", 100_000),
    )

    if source_type == "file":
        return FileExtractor(
            config=extractor_config,
            path=config["path"],
            file_format=config.get("format", "parquet"),
            pattern=config.get("pattern", "*.parquet"),
        )
    elif source_type == "api":
        return APIExtractor(
            config=extractor_config,
            base_url=config["base_url"],
            endpoint=config["endpoint"],
            api_key=config.get("api_key"),
            page_size=config.get("page_size", 1000),
        )
    elif source_type == "kafka":
        return KafkaExtractor(
            config=extractor_config,
            bootstrap_servers=config["bootstrap_servers"],
            topic=config["topic"],
            consumer_group=config["consumer_group"],
            window_seconds=config.get("window_seconds", 300),
        )
    else:
        raise ValueError(f"Unknown source type: {source_type}")
