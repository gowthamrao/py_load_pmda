import logging
from typing import List, Optional

import typer

from py_load_pmda import schemas
from py_load_pmda.config import load_config
from py_load_pmda.logging_config import setup_logging
from py_load_pmda.orchestrator import Orchestrator, get_db_adapter

app = typer.Typer()


@app.command()
def init_db() -> None:
    """
    Initialize the database by creating the core schema and state tables.
    """
    config = load_config()
    logging_config = config.get("logging", {})
    setup_logging(
        level=logging_config.get("level", "INFO"),
        log_format=logging_config.get("format", "text"),
    )

    logging.info("Initializing database...")
    try:
        db_config = config.get("database", {})
        adapter_type = db_config.get("type", "postgres")

        with get_db_adapter(adapter_type) as adapter:
            adapter.connect(db_config)
            adapter.ensure_schema(schemas.INGESTION_STATE_SCHEMA)
            adapter.commit()

        logging.info("✅ Database initialization complete.")
    except (FileNotFoundError, ConnectionError, ValueError) as e:
        # The adapter's rollback is useful here if the error is mid-transaction,
        # but the context manager will handle disconnection.
        logging.error(f"❌ Database initialization failed: {e}", exc_info=True)
        raise typer.Exit(code=1)


@app.command()
def run(
    dataset: str = typer.Option(..., "--dataset", help="The ID of the dataset to run."),
    mode: Optional[str] = typer.Option(None, "--mode", help="Load mode: 'full' or 'delta'. Overrides config."),
    year: Optional[int] = typer.Option(None, "--year", help="The fiscal year to process for approvals (if applicable)."),
    drug_name: Optional[List[str]] = typer.Option(None, "--drug-name", help="Name of a drug to search for package inserts. Can be specified multiple times.")
) -> None:
    """
    Run an ETL process for a specific dataset defined in the config.
    """
    # Validate arguments before doing anything else. Fail fast.
    if dataset == "approvals" and not year:
        typer.echo("Error: The '--year' option is required for the 'approvals' dataset.", err=True)
        raise typer.Exit(code=1)
    if dataset in ["package_inserts", "review_reports"] and not drug_name:
        typer.echo(
            f"Error: At least one '--drug-name' option is required for the '{dataset}' dataset.",
            err=True,
        )
        raise typer.Exit(code=1)

    try:
        config = load_config()
        orchestrator = Orchestrator(
            config=config,
            dataset=dataset,
            mode=mode,
            year=year,
            drug_name=drug_name,
        )
        orchestrator.run()
    except Exception as e:
        # The orchestrator will log the detailed exception.
        # The CLI's responsibility is to provide a clean top-level error message.
        logging.error(f"CLI-level error: An unexpected error occurred during the run: {e}")
        raise typer.Exit(code=1)


from rich.console import Console
from rich.table import Table

@app.command()
def status() -> None:
    """
    Check the status of the last runs from the ingestion_state table.
    """
    config = load_config()
    # Setup logging to show progress, but command output will go to stdout.
    setup_logging(level=config.get("logging", {}).get("level", "INFO"))

    console = Console()

    try:
        db_config = config.get("database", {})
        adapter_type = db_config.get("type", "postgres")
        schema_name = str(schemas.INGESTION_STATE_SCHEMA["schema_name"])

        with get_db_adapter(adapter_type) as adapter:
            adapter.connect(db_config)
            states = adapter.get_all_states(schema=schema_name)

        if not states:
            console.print("No ingestion state found in the database.")
            return

        table = Table(title="Ingestion Status", show_header=True, header_style="bold magenta")
        table.add_column("Dataset ID", style="dim", width=20)
        table.add_column("Status")
        table.add_column("Last Run (UTC)")
        table.add_column("Last Successful Run (UTC)")
        table.add_column("Pipeline Version")

        for state in sorted(states, key=lambda x: x.get('dataset_id', '')):
            status = state.get('status', 'UNKNOWN')
            status_style = "green" if status == "SUCCESS" else "red" if status == "FAILED" else "yellow"

            def format_date(dt):
                from datetime import datetime
                if not dt: return ""
                if isinstance(dt, str):
                    # Handle ISO format with 'Z' for UTC
                    if dt.endswith('Z'):
                        dt = dt[:-1] + '+00:00'
                    try:
                        dt_obj = datetime.fromisoformat(dt)
                        return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        return dt # Return original string if parsing fails
                elif isinstance(dt, datetime):
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                return str(dt)

            table.add_row(
                state.get('dataset_id', 'N/A'),
                f"[{status_style}]{status}[/{status_style}]",
                format_date(state.get('last_run_ts_utc')),
                format_date(state.get('last_successful_run_ts_utc')),
                state.get('pipeline_version', 'N/A')
            )

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]❌ Failed to get status: {e}[/bold red]")
        logging.debug("Full exception details:", exc_info=True)
        raise typer.Exit(code=1)

@app.command()
def check_config() -> None:
    """
    Validate configuration and database connectivity.
    """
    config = load_config()
    logging_config = config.get("logging", {})
    setup_logging(
        level=logging_config.get("level", "INFO"),
        log_format=logging_config.get("format", "text"),
    )

    logging.info("Checking configuration...")
    try:
        logging.info("✅ Configuration file loaded successfully.")
        db_config = config.get("database", {})
        adapter_type = db_config.get("type")
        if not adapter_type:
            raise ValueError("Database 'type' not specified in config.")

        logging.info(f"▶️ Database adapter type: {adapter_type}")

        with get_db_adapter(adapter_type) as adapter:
            logging.info("▶️ Attempting to connect to the database...")
            adapter.connect(db_config)
            adapter.commit()  # Test transaction by committing the implicit "BEGIN"

        logging.info("✅ Configuration check passed. Database connection successful.")
    except (FileNotFoundError, ConnectionError, ValueError, NotImplementedError) as e:
        logging.error(f"❌ Configuration check failed: {e}", exc_info=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
