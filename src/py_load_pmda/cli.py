import logging
from typing import Optional

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from py_load_pmda.config import load_config, get_db_connection_details
from py_load_pmda.extractor import ApprovalsExtractor
from py_load_pmda.interfaces import LoaderInterface
from py_load_pmda.parser import ApprovalsParser
from py_load_pmda.schemas import DB_SCHEMA
from py_load_pmda.transformer import ApprovalsTransformer
from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda.logging_config import setup_logging

app = typer.Typer()
console = Console()


def get_loader(db_type: str) -> LoaderInterface:
    """Factory function to get a database loader instance."""
    if db_type == "postgres":
        return PostgreSQLAdapter()
    # Add other database types here
    raise NotImplementedError(f"Database type '{db_type}' is not supported.")


@app.command()
def init_db():
    """
    Initialize the database schema and state tables.
    """
    setup_logging()
    logging.info("Initializing database...")
    try:
        config = load_config()
        connection_details = get_db_connection_details(config)
        loader = get_loader(connection_details["type"])
        loader.connect(connection_details)

        schema_name = config.get("database", {}).get("schema", "public")
        schema_def = {"schema_name": schema_name, "tables": DB_SCHEMA}

        loader.ensure_schema(schema_def)
        loader.commit()
        console.print("[green]Database initialized successfully.[/green]")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}", exc_info=True)
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)
    finally:
        if 'loader' in locals() and loader:
            loader.close()


@app.command()
def run(
    dataset: str = typer.Argument(..., help="The name of the dataset to process (e.g., 'approvals')."),
    year: Optional[int] = typer.Option(None, help="The year to process for datasets that are year-specific."),
    mode: str = typer.Option("incremental", help="Load mode: 'full' (overwrite) or 'incremental' (append/merge)."),
):
    """
    Run the ETL pipeline for a specific dataset.
    """
    setup_logging()
    logging.info(f"Starting ETL run for dataset '{dataset}' with mode '{mode}'...")

    if dataset.lower() == "approvals":
        if year is None:
            console.print("[red]Error: The 'approvals' dataset requires a --year to be specified.[/red]")
            raise typer.Exit(code=1)
        run_approvals_pipeline(year, mode)
    else:
        console.print(f"[red]Error: Dataset '{dataset}' is not yet implemented.[/red]")
        raise typer.Exit(code=1)


def run_approvals_pipeline(year: int, mode: str):
    """
    Orchestrates the ETL pipeline for the New Drug Approvals dataset.
    """
    loader = None
    try:
        config = load_config()
        connection_details = get_db_connection_details(config)
        schema_name = config.get("database", {}).get("schema", "public")

        loader = get_loader(connection_details["type"])
        loader.connect(connection_details)

        # 1. Extract
        extractor = ApprovalsExtractor()
        extracted_files = extractor.extract(year)

        if not extracted_files:
            console.print(f"[yellow]No files found for approvals in year {year}. Run complete.[/yellow]")
            return

        # 2. Parse
        parser = ApprovalsParser()
        all_data = []
        for file_path in extracted_files:
            df = parser.parse(file_path)
            if not df.empty:
                all_data.append(df)

        if not all_data:
            console.print(f"[yellow]No data could be parsed from the downloaded files. Run complete.[/yellow]")
            return

        raw_df = pd.concat(all_data, ignore_index=True)

        # 3. Transform
        transformer = ApprovalsTransformer()
        transformed_df = transformer.transform(raw_df)

        # 4. Load
        load_mode = "overwrite" if mode == "full" else "append"
        loader.bulk_load(
            data=transformed_df,
            target_table="pmda_approvals",
            schema=schema_name,
            mode=load_mode,
        )

        # 5. Update State & Commit
        # (State management logic would go here in a real incremental load)
        loader.commit()

        console.print(f"[green]Successfully processed and loaded {len(transformed_df)} records for approvals in {year}.[/green]")

    except Exception as e:
        logging.error(f"ETL run for approvals failed: {e}", exc_info=True)
        if loader:
            loader.rollback()
        console.print(f"[red]An error occurred during the ETL run: {e}[/red]")
        raise typer.Exit(code=1)
    finally:
        if loader:
            loader.close()


@app.command()
def status():
    """
    Check the status of the last runs for all datasets.
    """
    setup_logging(level=logging.WARNING) # Less verbose for status check
    loader = None
    try:
        config = load_config()
        connection_details = get_db_connection_details(config)
        schema_name = config.get("database", {}).get("schema", "public")
        loader = get_loader(connection_details["type"])
        loader.connect(connection_details)

        states = loader.get_all_states(schema=schema_name)
        if not states:
            console.print("[yellow]No run history found in the state management table.[/yellow]")
            return

        table = Table(title="ETL Run Status")
        table.add_column("Dataset ID", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Last Run (UTC)")
        table.add_column("Last Successful Run (UTC)")
        table.add_column("Pipeline Version")

        for state in states:
            status_color = "green" if state['status'] == 'SUCCESS' else "red"
            table.add_row(
                state['dataset_id'],
                f"[{status_color}]{state['status']}[/{status_color}]",
                str(state.get('last_run_ts_utc')),
                str(state.get('last_successful_run_ts_utc')),
                state.get('pipeline_version'),
            )
        console.print(table)

    except Exception as e:
        console.print(f"[red]Error checking status: {e}[/red]")
        raise typer.Exit(code=1)
    finally:
        if loader:
            loader.close()


@app.command()
def check_config():
    """
    Validate configuration and database connectivity.
    """
    setup_logging()
    logging.info("Checking configuration...")
    loader = None
    try:
        config = load_config()
        console.print("[green]Configuration file loaded successfully.[/green]")

        connection_details = get_db_connection_details(config)
        console.print(f"Attempting to connect to {connection_details['type']} database...")

        loader = get_loader(connection_details["type"])
        loader.connect(connection_details)
        console.print("[green]Database connection successful.[/green]")

    except Exception as e:
        console.print(f"[red]Configuration check failed: {e}[/red]")
        raise typer.Exit(code=1)
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    app()
