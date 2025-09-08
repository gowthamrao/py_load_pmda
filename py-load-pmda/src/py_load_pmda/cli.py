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


@app.command()
def run(
    dataset: str = typer.Option(..., "--dataset", help="The ID of the dataset to run."),
    mode: str = typer.Option("delta", "--mode", help="Load mode: 'full' or 'delta'."),
):
    """
    Run a specific dataset load.
    """
    print(f"Running load for dataset '{dataset}' in '{mode}' mode.")


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
