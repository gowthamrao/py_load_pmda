import typer

app = typer.Typer()


@app.command()
def init_db():
    """
    Initialize the database by creating the core schema and state tables.
    """
    print("Initializing database...")
    try:
        config = load_config()
        db_config = config.get("database", {})

        adapter = PostgreSQLAdapter()
        adapter.connect(db_config)

        # Define the schema for the state management table
        state_schema = {
            "schema_name": "public",  # Core table, can be in public schema
            "tables": {
                "ingestion_state": {
                    "columns": {
                        "dataset_id": "VARCHAR(100) NOT NULL",
                        "last_run_ts_utc": "TIMESTAMPTZ",
                        "last_successful_run_ts_utc": "TIMESTAMPTZ",
                        "status": "VARCHAR(50)",
                        "last_watermark": "JSONB",
                        "pipeline_version": "VARCHAR(50)",
                    },
                    "primary_key": "dataset_id",
                }
            }
        }

        adapter.ensure_schema(state_schema)
        print("\n✅ Database initialization complete.")

    except (FileNotFoundError, ConnectionError, ValueError) as e:
        print(f"\n❌ Database initialization failed: {e}")
        raise typer.Exit(code=1)


from py_load_pmda.extractor import ApprovalsExtractor
from py_load_pmda.parser import ApprovalsParser
from py_load_pmda.transformer import ApprovalsTransformer

def get_db_adapter(db_type: str):
    """Factory function for database adapters."""
    if db_type == "postgres":
        return PostgreSQLAdapter()
    # In the future, add other adapters here
    # elif db_type == "redshift":
    #     return RedshiftAdapter()
    raise NotImplementedError(f"Database type '{db_type}' is not supported.")

@app.command()
def run(
    dataset: str = typer.Option(..., "--dataset", help="The ID of the dataset to run."),
    mode: str = typer.Option("full", "--mode", help="Load mode: 'full' or 'delta'."),
    year: int = typer.Option(2025, "--year", help="The fiscal year to process for approvals."),
):
    """
    Run an ETL process for a specific dataset.
    """
    print(f"Starting ETL run for dataset '{dataset}' in '{mode}' mode.")

    try:
        config = load_config()
        db_config = config.get("database", {})
        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)

        if dataset == "approvals":
            # Define schema for the approvals table
            approvals_schema = {
                "schema_name": "public",
                "tables": {
                    "pmda_approvals": {
                        "columns": {
                            "approval_id": "VARCHAR(100)",
                            "application_type": "VARCHAR(50)",
                            "brand_name_jp": "TEXT",
                            "generic_name_jp": "TEXT",
                            "applicant_name_jp": "TEXT",
                            "approval_date": "DATE",
                            "indication": "TEXT",
                            "review_report_url": "TEXT",
                            "raw_data_full": "JSONB",
                            "_meta_load_ts_utc": "TIMESTAMPTZ",
                            "_meta_source_content_hash": "VARCHAR(64)",
                            "_meta_source_url": "TEXT",
                            "_meta_pipeline_version": "VARCHAR(50)",
                        },
                        "primary_key": "approval_id, approval_date", # Composite key
                    }
                }
            }
            adapter.ensure_schema(approvals_schema)

            # 1. Extract
            extractor = ApprovalsExtractor()
            file_path, source_url = extractor.extract(year=year)

            # 2. Parse
            parser = ApprovalsParser()
            raw_df = parser.parse(file_path)

            # 3. Transform
            transformer = ApprovalsTransformer(source_url=source_url)
            transformed_df = transformer.transform(raw_df)

            # 4. Load
            load_mode = "overwrite" if mode == "full" else "append"
            adapter.bulk_load(
                data=transformed_df,
                target_table="pmda_approvals",
                schema="public",
                mode=load_mode,
            )

        else:
            print(f"Error: Dataset '{dataset}' is not supported.")
            raise typer.Exit(code=1)

        print(f"\n✅ ETL run for dataset '{dataset}' completed successfully.")

    except Exception as e:
        print(f"\n❌ ETL run failed: {e}")
        import traceback
        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def status():
    """
    Check the status of the last runs.
    """
    print("Checking status...")


from py_load_pmda.config import load_config
from py_load_pmda.adapters.postgres import PostgreSQLAdapter

@app.command()
def check_config():
    """
    Validate configuration and database connectivity.
    """
    print("Checking configuration...")
    try:
        config = load_config()
        print("Configuration file loaded successfully.")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)

    db_config = config.get("database", {})
    adapter_type = db_config.get("type")

    if not adapter_type:
        print("Error: Database 'type' not specified in config.")
        raise typer.Exit(code=1)

    print(f"Database adapter type: {adapter_type}")

    # Adapter factory - for now, only supports postgres
    if adapter_type == "postgres":
        adapter = PostgreSQLAdapter()
    else:
        print(f"Error: Unsupported database type '{adapter_type}'")
        raise typer.Exit(code=1)

    try:
        print("Attempting to connect to the database...")
        adapter.connect(db_config)
        print("\n✅ Configuration check passed. Database connection successful.")
    except ConnectionError as e:
        print(f"\n❌ Configuration check failed. {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
