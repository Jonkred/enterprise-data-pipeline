"""Data lineage tracking for pipeline observability."""

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LineageEvent:
    """A single lineage event in the pipeline."""

    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    run_id: str = ""
    pipeline_name: str = ""
    event_type: str = ""  # source_extract, transform, load, quality_check
    source: str = ""
    destination: str = ""
    entity: str = ""  # table, file, topic name
    row_count: int = 0
    duration_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class LineageTracker:
    """Track data lineage with SQLite backend."""

    def __init__(self, db_path: str = "data/lineage.db") -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._logger = get_logger(__name__)

    def _init_db(self) -> None:
        """Initialize lineage database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lineage_events (
                    event_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    pipeline_name TEXT,
                    event_type TEXT,
                    source TEXT,
                    destination TEXT,
                    entity TEXT,
                    row_count INTEGER,
                    duration_ms REAL,
                    metadata TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_run_id ON lineage_events(run_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pipeline ON lineage_events(pipeline_name)
                """
            )
            conn.commit()

    def record(self, event: LineageEvent) -> None:
        """Record a lineage event."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO lineage_events
                (event_id, timestamp, run_id, pipeline_name, event_type, source, destination, entity, row_count, duration_ms, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.timestamp,
                    event.run_id,
                    event.pipeline_name,
                    event.event_type,
                    event.source,
                    event.destination,
                    event.entity,
                    event.row_count,
                    event.duration_ms,
                    json.dumps(event.metadata),
                ),
            )
            conn.commit()

        self._logger.info(
            "Lineage event recorded",
            event_type=event.event_type,
            entity=event.entity,
            rows=event.row_count,
        )

    def get_run_lineage(self, run_id: str) -> list[dict[str, Any]]:
        """Get all lineage events for a pipeline run."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM lineage_events WHERE run_id = ? ORDER BY timestamp",
                (run_id,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_entity_impact(self, entity: str) -> list[dict[str, Any]]:
        """Find all downstream dependencies of an entity."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT DISTINCT destination, pipeline_name
                FROM lineage_events
                WHERE source = ? AND destination IS NOT NULL
                """,
                (entity,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_pipeline_stats(self, pipeline_name: str, days: int = 7) -> dict[str, Any]:
        """Get pipeline execution statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT run_id) as total_runs,
                    SUM(row_count) as total_rows,
                    AVG(duration_ms) as avg_duration,
                    MAX(timestamp) as last_run
                FROM lineage_events
                WHERE pipeline_name = ?
                AND timestamp > datetime('now', '-{} days')
                """.format(days),
                (pipeline_name,),
            )
            row = cursor.fetchone()
            return {
                "pipeline": pipeline_name,
                "total_runs": row[0] if row else 0,
                "total_rows": row[1] if row else 0,
                "avg_duration_ms": round(row[2], 2) if row and row[2] else 0,
                "last_run": row[3] if row else None,
            }
