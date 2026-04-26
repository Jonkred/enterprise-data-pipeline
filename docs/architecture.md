# Architecture Documentation

## System Design

### Design Principles

1. **Modularity** — Each source, transformation, and destination is a pluggable component
2. **Configuration-Driven** — Pipeline behavior controlled by YAML, not code changes
3. **Observability First** — Every operation is logged and tracked in lineage
4. **Fail Gracefully** — Quality issues are logged; pipeline continues unless configured otherwise
5. **Cost Conscious** — Batch sizing, incremental loads, and compression minimize compute and storage

### Data Flow

```
Source Systems (ERP/CRM/IoT)
    │
    ├──► Extractor (Abstract Base Class)
    │      ├── FileExtractor ──► CSV/Parquet/JSON
    │      ├── APIExtractor ──► REST API + pagination
    │      └── KafkaExtractor ──► Streaming + windowing
    │
    ▼
Quality Gates (Expectation Suite)
    │      ├── Row count validation
    │      ├── Null checks
    │      ├── Range validation
    │      └── Uniqueness + regex
    │
    ▼
Transformation Pipeline (Sequential)
    │      ├── DeduplicationTransformer
    │      ├── EnrichmentTransformer
    │      ├── AggregationTransformer
    │      └── TypeCastTransformer
    │
    ▼
Loaders (Parallel Capable)
    │      ├── SQLLoader ──► PostgreSQL/Snowflake/SQLite
    │      └── ParquetLoader ──► S3/Local with partitioning
    │
    ▼
Lineage DB (SQLite)
    │      └── Full audit: source → transform → destination
```

### Scalability Considerations

| Component | Current | Scale Path |
|-----------|---------|------------|
| Extraction | Pandas (1M rows/min) | PySpark for 10M+ |
| Transformation | In-memory | Dask/Spark for > RAM |
| Loading | SQLAlchemy batch | COPY commands for bulk |
| Quality | Pandas validation | GE Cloud for distributed |
| Orchestration | Airflow Celery | KubernetesExecutor |

### Security

- API keys via environment variables (never committed)
- Database credentials via connection strings in `.env`
- Production: integrate HashiCorp Vault or AWS Secrets Manager

### Error Handling

```
Extraction Error → Log + Retry (3x) → Alert if exhausted
Quality Failure  → Log + Continue (configurable) → Report
Transform Error  → Log + Stop Pipeline → Alert
Load Error       → Log + Retry (2x) → Alert if exhausted
```
