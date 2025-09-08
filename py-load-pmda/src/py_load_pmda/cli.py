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
    adapter = None
    try:
        config = load_config()
        db_config = config.get("database", {})
        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)
        adapter.ensure_schema(schemas.INGESTION_STATE_SCHEMA)
        adapter.commit()
        print("\n✅ Database initialization complete.")
    except (FileNotFoundError, ConnectionError, ValueError) as e:
        if adapter:
            adapter.rollback()
        print(f"\n❌ Database initialization failed: {e}")
        raise typer.Exit(code=1)
    finally:
        if adapter:
            adapter.close()


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
    adapter = None
    status = "FAILED"
    new_state = {}

    try:
        # 1. Load Configuration
        config = load_config()
        db_config = config.get("database", {})
        dataset_configs = config.get("datasets", {})
        if dataset not in dataset_configs:
            raise ValueError(f"Dataset '{dataset}' not found in config.yaml.")
        ds_config = dataset_configs[dataset]

        # 2. Initialize and Connect Database Adapter
        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)

        # 3. Ensure Target Table Schema Exists
        target_schema_def = schemas.DATASET_SCHEMAS.get(dataset)
        if not target_schema_def:
            raise ValueError(f"Schema for dataset '{dataset}' not found in schemas.py.")
        adapter.ensure_schema(target_schema_def)

        # 4. Get Current State
        # The schema for the state table is assumed to be 'public' for now.
        state_schema = schemas.INGESTION_STATE_SCHEMA["schema_name"]
        last_state = adapter.get_latest_state(dataset, schema=state_schema)
        print(f"Last state for '{dataset}': {last_state}")

        # 5. Get ETL Classes from Registry
        extractor_class = AVAILABLE_EXTRACTORS.get(ds_config["extractor"])
        parser_class = AVAILABLE_PARSERS.get(ds_config["parser"])
        transformer_class = AVAILABLE_TRANSFORMERS.get(ds_config["transformer"])
        if not all([extractor_class, parser_class, transformer_class]):
            raise ValueError(f"One or more ETL classes for '{dataset}' could not be found.")

        # 6. Execute ETL
        # A. Extract
        print(f"--- Running Extractor: {ds_config['extractor']} ---")
        extractor_instance = extractor_class()
        # Pass state to extractor for delta detection
        extract_args = {"last_state": last_state}
        if dataset == "approvals":
            extract_args['year'] = year

        file_path, source_url, new_state = extractor_instance.extract(**extract_args)

        # If the new state is the same as the old state, we can stop.
        if new_state == last_state and last_state:
             print("Data source has not changed since last run. Pipeline will stop.")
             status = "SUCCESS"
             # We still want to update the 'last_run_ts_utc' in the state table
             adapter.update_state(dataset, state=new_state, status=status, schema=schemas.INGESTION_STATE_SCHEMA["schema_name"])
             adapter.commit()
             return

        # B. Parse
        print(f"--- Running Parser: {ds_config['parser']} ---")
        parser_instance = parser_class()
        raw_df = parser_instance.parse(file_path)

        # C. Transform
        print(f"--- Running Transformer: {ds_config['transformer']} ---")
        transformer_instance = transformer_class(source_url=source_url)
        transformed_df = transformer_instance.transform(raw_df)

        # 7. Load
        load_mode = mode or ds_config.get("load_mode", "overwrite")
        schema_name = ds_config["schema_name"]
        table_name = ds_config["table_name"]
        print(f"--- Loading data to {schema_name}.{table_name} (mode: {load_mode}) ---")

        if transformed_df.empty:
            print("Transformed DataFrame is empty. Nothing to load.")
        elif load_mode == "merge":
            primary_keys = ds_config.get("primary_key")
            if not primary_keys:
                raise ValueError(f"load_mode 'merge' requires 'primary_key' in config for dataset '{dataset}'.")

            staging_table_name = f"staging_{table_name}"

            # Create a temporary schema for the staging table
            staging_schema = {
                "schema_name": schema_name,
                "tables": {
                    staging_table_name: {
                        "columns": target_schema_def["tables"][table_name]["columns"]
                    }
                }
            }

            try:
                print(f"Creating staging table: {schema_name}.{staging_table_name}")
                adapter.ensure_schema(staging_schema)

                print(f"Loading data into staging table...")
                adapter.bulk_load(
                    data=transformed_df,
                    target_table=staging_table_name,
                    schema=schema_name,
                    mode="overwrite", # Always overwrite the staging table
                )

                print("Executing merge operation...")
                adapter.execute_merge(
                    staging_table=staging_table_name,
                    target_table=table_name,
                    primary_keys=primary_keys,
                    schema=schema_name,
                )
            finally:
                # Ensure the staging table is always dropped
                print(f"Dropping staging table: {schema_name}.{staging_table_name}")
                adapter.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{staging_table_name};")

        else: # Handles 'append' and 'overwrite'
            adapter.bulk_load(
                data=transformed_df,
                target_table=table_name,
                schema=schema_name,
                mode=load_mode,
            )

        # 8. Update State
        status = "SUCCESS"
        print(f"\n✅ ETL run for dataset '{dataset}' completed successfully.")

    except Exception as e:
        print(f"\n❌ ETL run failed: {e}")
        if adapter:
            adapter.rollback()
        import traceback
        traceback.print_exc()
        raise typer.Exit(code=1)

    finally:
        if adapter:
            # Always update the state, whether success or failure
            adapter.update_state(dataset, state=new_state, status=status, schema=schemas.INGESTION_STATE_SCHEMA["schema_name"])
            adapter.commit()
            adapter.close()


@app.command()
def status():
    """
    Check the status of the last runs from the ingestion_state table.
    """
    print("--- Ingestion Status ---")
    adapter = None
    try:
        config = load_config()
        db_config = config.get("database", {})
        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)
        states = adapter.get_all_states(schema=schemas.INGESTION_STATE_SCHEMA["schema_name"])

        if not states:
            print("No ingestion state found in the database.")
            return

        import pandas as pd
        df = pd.DataFrame(states)
        for col in ['last_run_ts_utc', 'last_successful_run_ts_utc']:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.drop(columns=['last_watermark'], inplace=True, errors='ignore')
        print(df.to_string())

    except Exception as e:
        print(f"\n❌ Failed to get status: {e}")
        raise typer.Exit(code=1)
    finally:
        if adapter:
            adapter.close()

@app.command()
def check_config():
    """
    Validate configuration and database connectivity.
    """
    print("Checking configuration...")
    adapter = None
    try:
        config = load_config()
        print("✅ Configuration file loaded successfully.")
        db_config = config.get("database", {})
        adapter_type = db_config.get("type")
        if not adapter_type:
            raise ValueError("Database 'type' not specified in config.")

        print(f"▶️ Database adapter type: {adapter_type}")
        adapter = get_db_adapter(adapter_type)
        print("▶️ Attempting to connect to the database...")
        adapter.connect(db_config)
        adapter.commit() # Test transaction
        print("✅ Configuration check passed. Database connection successful.")
    except (FileNotFoundError, ConnectionError, ValueError, NotImplementedError) as e:
        if adapter:
            adapter.rollback()
        print(f"❌ Configuration check failed. {e}")
        raise typer.Exit(code=1)
    finally:
        if adapter:
            adapter.close()


if __name__ == "__main__":
    app()
