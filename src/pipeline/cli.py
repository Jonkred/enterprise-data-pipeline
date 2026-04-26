"""Command-line interface for the data pipeline."""

from pathlib import Path
from typing import Optional
from uuid import uuid4

import typer
import yaml
from rich.console import Console
from rich.table import Table

from pipeline.config import get_settings
from pipeline.extractors import create_extractor
from pipeline.lineage import LineageEvent, LineageTracker
from pipeline.loaders import create_loader
from pipeline.quality import QualityEngine
from pipeline.transformers import (
    AggregationTransformer,
    DeduplicationTransformer,
    PipelineTransformer,
    TypeCastTransformer,
)
from pipeline.utils.logger import configure_logging, get_logger

app = typer.Typer(help="Enterprise Data Pipeline CLI", rich_markup_mode="rich")
console = Console()
logger = get_logger(__name__)


@app.command()
def run(
    config: str = typer.Option("configs/pipeline.yaml", help="Path to pipeline configuration file"),
    dry_run: bool = typer.Option(False, help="Validate without executing"),
) -> None:
    """Execute the full data pipeline."""
    settings = get_settings()
    configure_logging(settings.log_level)

    run_id = str(uuid4())[:8]
    logger.info("Pipeline starting", run_id=run_id, config=config)

    # Load pipeline configuration
    with open(config) as f:
        pipeline_config = yaml.safe_load(f)

    if dry_run:
        logger.info("DRY RUN MODE - validating only")

    tracker = LineageTracker() if settings.pipeline.enable_lineage_tracking else None

    # Phase 1: Extract
    console.rule("[bold blue]Phase 1: Data Extraction")
    extracted_data = {}

    for source_name, source_cfg in pipeline_config["pipeline"]["sources"].items():
        logger.info("Extracting source", source=source_name)
        extractor = create_extractor(
            source_cfg["type"],
            {
                "name": source_name,
                **{k: v for k, v in source_cfg.items() if k != "type"},
            },
        )

        if not dry_run:
            df = extractor.extract()
            extracted_data[source_name] = df

            if tracker:
                tracker.record(
                    LineageEvent(
                        run_id=run_id,
                        pipeline_name=pipeline_config["pipeline"]["name"],
                        event_type="source_extract",
                        source=source_name,
                        entity=source_name,
                        row_count=len(df),
                        metadata=extractor.get_metadata(),
                    )
                )

            console.print(f"[green]✓[/green] {source_name}: {len(df):,} rows extracted")

    if dry_run:
        console.print("[yellow]Dry run complete - extraction validated[/yellow]")
        return

    # Phase 2: Transform
    console.rule("[bold blue]Phase 2: Transformation")
    transformed_data = {}

    for source_name, df in extracted_data.items():
        logger.info("Transforming", source=source_name, rows=len(df))

        pipeline = PipelineTransformer()

        # Apply source-specific transformations from config
        for tx_config in pipeline_config["pipeline"].get("transformations", []):
            tx_name = tx_config["name"]

            if tx_config["type"] == "deduplication":
                pipeline.add(
                    DeduplicationTransformer(
                        key_columns=tx_config.get("key_columns", ["id"]),
                        timestamp_column=tx_config.get("timestamp_column", "updated_at"),
                    )
                )
            elif tx_config["type"] == "aggregation":
                pipeline.add(
                    AggregationTransformer(
                        group_by=tx_config.get("group_by", []),
                        metrics=tx_config.get("metrics", []),
                    )
                )
            elif tx_config["type"] == "type_cast":
                pipeline.add(TypeCastTransformer(type_map=tx_config.get("type_map", {})))

        result = pipeline.run(df)
        transformed_data[source_name] = result

        if tracker:
            tracker.record(
                LineageEvent(
                    run_id=run_id,
                    pipeline_name=pipeline_config["pipeline"]["name"],
                    event_type="transform",
                    source=source_name,
                    destination=f"{source_name}_transformed",
                    entity=source_name,
                    row_count=len(result),
                    metadata={"transformations": [t.name for t in pipeline.transformers]},
                )
            )

        console.print(f"[green]✓[/green] {source_name}: {len(result):,} rows after transform")

    # Phase 3: Quality Gates
    console.rule("[bold blue]Phase 3: Data Quality")
    quality_engine = QualityEngine()
    quality_passed = True

    for suite_config in pipeline_config["pipeline"].get("quality", {}).get("suites", []):
        suite_name = suite_config["name"]
        target_source = suite_config.get("target", list(extracted_data.keys())[0])

        if target_source in transformed_data:
            result = quality_engine.run_suite(
                transformed_data[target_source],
                suite_name,
                suite_config.get("expectations", []),
            )

            status = "[green]PASS[/green]" if result.success else "[red]FAIL[/red]"
            console.print(f"{status} {suite_name}: {result.passed}/{result.evaluated} passed")

            if not result.success:
                quality_passed = False

    # Phase 4: Load
    console.rule("[bold blue]Phase 4: Load to Warehouse")
    total_loaded = 0

    for dest_name, dest_config in pipeline_config["pipeline"]["destinations"].items():
        loader = create_loader(dest_config["type"], dest_config)

        for source_name, df in transformed_data.items():
            table_name = f"{source_name}_facts"
            rows = loader.load(df, table_name)
            total_loaded += rows

            if tracker:
                tracker.record(
                    LineageEvent(
                        run_id=run_id,
                        pipeline_name=pipeline_config["pipeline"]["name"],
                        event_type="load",
                        source=source_name,
                        destination=dest_name,
                        entity=table_name,
                        row_count=rows,
                    )
                )

            console.print(f"[green]✓[/green] Loaded {rows:,} rows to {dest_name}.{table_name}")

    # Summary
    console.rule("[bold green]Pipeline Complete")
    summary = Table(title=f"Run Summary: {run_id}")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", style="magenta")
    summary.add_row("Sources Processed", str(len(extracted_data)))
    summary.add_row("Total Rows Extracted", str(sum(len(df) for df in extracted_data.values())))
    summary.add_row("Total Rows Loaded", str(total_loaded))
    summary.add_row("Quality Gates", "PASS" if quality_passed else "FAIL")

    if tracker:
        stats = tracker.get_pipeline_stats(pipeline_config["pipeline"]["name"], days=1)
        summary.add_row("Pipeline Runs (24h)", str(stats["total_runs"]))

    console.print(summary)
    logger.info("Pipeline complete", run_id=run_id, total_loaded=total_loaded)


@app.command()
def lineage(
    run_id: Optional[str] = typer.Option(None, help="Show lineage for specific run"),
    pipeline: Optional[str] = typer.Option(None, help="Filter by pipeline name"),
    entity: Optional[str] = typer.Option(None, help="Show impact analysis for entity"),
) -> None:
    """Query data lineage and impact analysis."""
    tracker = LineageTracker()

    if entity:
        impacts = tracker.get_entity_impact(entity)
        table = Table(title=f"Downstream Impact: {entity}")
        table.add_column("Destination")
        table.add_column("Pipeline")
        for impact in impacts:
            table.add_row(impact["destination"], impact["pipeline_name"])
        console.print(table)
        return

    if run_id:
        events = tracker.get_run_lineage(run_id)
        table = Table(title=f"Lineage: Run {run_id}")
        table.add_column("Event")
        table.add_column("Entity")
        table.add_column("Rows")
        table.add_column("Duration")
        for event in events:
            table.add_row(
                event["event_type"],
                event["entity"],
                str(event["row_count"]),
                f"{event['duration_ms']:.0f}ms",
            )
        console.print(table)
        return

    if pipeline:
        stats = tracker.get_pipeline_stats(pipeline)
        table = Table(title=f"Pipeline Stats: {pipeline}")
        table.add_column("Metric")
        table.add_column("Value")
        for key, value in stats.items():
            table.add_row(key, str(value))
        console.print(table)
        return

    console.print("[yellow]Please provide --run-id, --pipeline, or --entity[/yellow]")


@app.command()
def validate(
    config: str = typer.Option("configs/pipeline.yaml", help="Pipeline config to validate"),
) -> None:
    """Validate pipeline configuration without executing."""
    console.rule("[bold blue]Validating Pipeline Configuration")

    try:
        with open(config) as f:
            cfg = yaml.safe_load(f)

        # Check root-level 'pipeline' key exists
        if "pipeline" not in cfg:
            console.print("[red]✗ Missing required root key: 'pipeline'[/red]")
            raise typer.Exit(1)

        pipeline_cfg = cfg["pipeline"]
        required_pipeline_keys = ["sources", "destinations"]
        for key in required_pipeline_keys:
            if key not in pipeline_cfg:
                console.print(f"[red]✗ Missing required key: pipeline.{key}[/red]")
                raise typer.Exit(1)

        console.print("[green]✓ Configuration structure valid[/green]")
        console.print(f"[green]✓ Sources: {list(cfg['pipeline']['sources'].keys())}[/green]")
        console.print(f"[green]✓ Destinations: {list(cfg['pipeline']['destinations'].keys())}[/green]")

        # Validate extractors can be instantiated with actual config
        for source_name, source_cfg in cfg["pipeline"]["sources"].items():
            try:
                extractor_config = {
                    "name": source_name,
                    **{k: v for k, v in source_cfg.items() if k != "type"},
                }
                extractor = create_extractor(source_cfg["type"], extractor_config)
                console.print(f"[green]✓ Extractor '{source_name}' validated[/green]")
            except Exception as e:
                console.print(f"[red]✗ Extractor '{source_name}' failed: {e}[/red]")

        console.print("[bold green]All validations passed[/bold green]")

    except Exception as e:
        console.print(f"[red]Validation failed: {e}[/red]")
        raise typer.Exit(1)


def main() -> None:
    """Entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
