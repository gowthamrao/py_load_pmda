import typer
import importlib

from py_load_pmda.config import load_config
from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda import extractor, parser, transformer
from py_load_pmda import schemas

app = typer.Typer()

# --- ETL Class Registries ---
# This allows us to look up the specific ETL class based on a string from the config
AVAILABLE_EXTRACTORS = {
    "ApprovalsExtractor": extractor.ApprovalsExtractor,
    "JaderExtractor": extractor.JaderExtractor,
}

AVAILABLE_PARSERS = {
    "ApprovalsParser": parser.ApprovalsParser,
    "JaderParser": parser.JaderParser,
}

AVAILABLE_TRANSFORMERS = {
    "ApprovalsTransformer": transformer.ApprovalsTransformer,
    "JaderTransformer": transformer.JaderTransformer,
}


def get_db_adapter(db_type: str):
    """Factory function for database adapters."""
    if db_type == "postgres":
        return PostgreSQLAdapter()
    raise NotImplementedError(f"Database type '{db_type}' is not supported.")


@app.command()
def init_db():
    """
    Initialize the database by creating the core schema and state tables.
    """
    print("Initializing database...")
    try:
        config = load_config()
        db_config = config.get("database", {})

        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)

        # Use the centralized schema definition
        adapter.ensure_schema(schemas.INGESTION_STATE_SCHEMA)
        print("\n✅ Database initialization complete.")

    except (FileNotFoundError, ConnectionError, ValueError) as e:
        print(f"\n❌ Database initialization failed: {e}")
        raise typer.Exit(code=1)


@app.command()
def run(
    dataset: str = typer.Option(..., "--dataset", help="The ID of the dataset to run."),
    mode: str = typer.Option(None, "--mode", help="Load mode: 'full' or 'delta'. Overrides config."),
    year: int = typer.Option(2025, "--year", help="The fiscal year to process for approvals (if applicable)."),
):
    """
    Run an ETL process for a specific dataset defined in the config.
    """
    print(f"Starting ETL run for dataset '{dataset}'.")

    try:
        # 1. Load Configuration
        config = load_config()
        db_config = config.get("database", {})

        dataset_configs = config.get("datasets", {})
        if dataset not in dataset_configs:
            print(f"❌ Error: Dataset '{dataset}' not found in config.yaml.")
            raise typer.Exit(code=1)

        ds_config = dataset_configs[dataset]

        # 2. Initialize Database Adapter
        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)

        # 3. Ensure Target Table Schema Exists
        target_schema = schemas.DATASET_SCHEMAS.get(dataset)
        if not target_schema:
            print(f"❌ Error: Schema for dataset '{dataset}' not found in schemas.py.")
            raise typer.Exit(code=1)
        adapter.ensure_schema(target_schema)

        # 4. Get ETL Classes from Registry
        extractor_class = AVAILABLE_EXTRACTORS.get(ds_config["extractor"])
        parser_class = AVAILABLE_PARSERS.get(ds_config["parser"])
        transformer_class = AVAILABLE_TRANSFORMERS.get(ds_config["transformer"])

        if not all([extractor_class, parser_class, transformer_class]):
            print(f"❌ Error: One or more ETL classes for dataset '{dataset}' could not be found.")
            raise typer.Exit(code=1)

        # 5. Execute ETL
        # A. Extract
        print(f"--- Running Extractor: {ds_config['extractor']} ---")
        # Note: We need a way to pass dataset-specific args like 'year'
        # For now, we'll handle it conditionally, but a better approach
        # might be to pass all optional CLI args as a dict.
        if dataset == "approvals":
            extractor_instance = extractor_class()
            file_path, source_url = extractor_instance.extract(year=year)
        else:
            # Generic instantiation for future datasets
            extractor_instance = extractor_class()
            file_path, source_url = extractor_instance.extract()

        # B. Parse
        print(f"--- Running Parser: {ds_config['parser']} ---")
        parser_instance = parser_class()
        raw_df = parser_instance.parse(file_path)

        # C. Transform
        print(f"--- Running Transformer: {ds_config['transformer']} ---")
        transformer_instance = transformer_class(source_url=source_url)
        transformed_df = transformer_instance.transform(raw_df)

        # 6. Load
        print(f"--- Loading data to {ds_config['schema_name']}.{ds_config['table_name']} ---")
        load_mode = mode or ds_config.get("load_mode", "overwrite")
        adapter.bulk_load(
            data=transformed_df,
            target_table=ds_config["table_name"],
            schema=ds_config["schema_name"],
            mode=load_mode,
        )

        print(f"\n✅ ETL run for dataset '{dataset}' completed successfully.")

    except Exception as e:
        print(f"\n❌ ETL run failed: {e}")
        import traceback
        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def status():
    """
    Check the status of the last runs from the ingestion_state table.
    """
    print("--- Ingestion Status ---")
    try:
        config = load_config()
        db_config = config.get("database", {})
        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)

        # Assuming state table is in the 'public' schema, which is a reasonable default.
        # A more advanced implementation might make this configurable.
        states = adapter.get_all_states(schema="public")

        if not states:
            print("No ingestion state found in the database.")
            return

        # Use pandas to easily format the output
        import pandas as pd
        df = pd.DataFrame(states)

        # Format for readability
        df['last_run_ts_utc'] = pd.to_datetime(df['last_run_ts_utc']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['last_successful_run_ts_utc'] = pd.to_datetime(df['last_successful_run_ts_utc']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.drop(columns=['last_watermark'], inplace=True, errors='ignore')

        print(df.to_string())

    except Exception as e:
        print(f"\n❌ Failed to get status: {e}")
        raise typer.Exit(code=1)

@app.command()
def check_config():
    """
    Validate configuration and database connectivity.
    """
    print("Checking configuration...")
    try:
        config = load_config()
        print("✅ Configuration file loaded successfully.")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        raise typer.Exit(code=1)

    db_config = config.get("database", {})
    adapter_type = db_config.get("type")

    if not adapter_type:
        print("❌ Error: Database 'type' not specified in config.")
        raise typer.Exit(code=1)

    print(f"▶️ Database adapter type: {adapter_type}")

    try:
        adapter = get_db_adapter(adapter_type)
        print("▶️ Attempting to connect to the database...")
        adapter.connect(db_config)
        print("✅ Configuration check passed. Database connection successful.")
    except (ConnectionError, NotImplementedError) as e:
        print(f"❌ Configuration check failed. {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
