"""Application configuration using Pydantic Settings."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "pipeline"
    postgres_password: str = "changeme"
    postgres_db: str = "warehouse"
    sqlite_path: str = "data/warehouse.db"


class SourceSettings(BaseSettings):
    """Data source connection settings."""

    erp_data_path: str = "data/sources/erp"
    crm_api_base_url: str = "https://api.crm.example.com/v1"
    crm_api_key: str = Field(default="", repr=False)
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_iot_topic: str = "iot.sensor.data"


class PipelineSettings(BaseSettings):
    """Pipeline execution settings."""

    batch_size: int = 100_000
    max_workers: int = 4
    enable_schema_evolution: bool = True
    enable_quality_checks: bool = True
    quality_threshold: float = 0.95
    enable_lineage_tracking: bool = True


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    environment: str = "development"
    log_level: str = "INFO"

    db: DatabaseSettings = DatabaseSettings()
    sources: SourceSettings = SourceSettings()
    pipeline: PipelineSettings = PipelineSettings()

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment.lower() == "development"

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment.lower() == "production"

    @property
    def warehouse_connection_string(self) -> str:
        """Build database connection string."""
        if self.is_development:
            Path(self.db.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            return f"sqlite:///{self.db.sqlite_path}"
        return (
            f"postgresql://{self.db.postgres_user}:{self.db.postgres_password}"
            f"@{self.db.postgres_host}:{self.db.postgres_port}/{self.db.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
