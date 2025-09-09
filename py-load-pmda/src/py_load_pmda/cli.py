import typer
import importlib
from typing import List

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
    "PackageInsertsExtractor": extractor.PackageInsertsExtractor,
    "ReviewReportsExtractor": extractor.ReviewReportsExtractor,
}

AVAILABLE_PARSERS = {
    "ApprovalsParser": parser.ApprovalsParser,
    "JaderParser": parser.JaderParser,
    "PackageInsertsParser": parser.PackageInsertsParser,
    "ReviewReportsParser": parser.ReviewReportsParser,
}

AVAILABLE_TRANSFORMERS = {
    "ApprovalsTransformer": transformer.ApprovalsTransformer,
    "JaderTransformer": transformer.JaderTransformer,
    "PackageInsertsTransformer": transformer.PackageInsertsTransformer,
    "ReviewReportsTransformer": transformer.ReviewReportsTransformer,
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
    year: int = typer.Option(None, "--year", help="The fiscal year to process for approvals (if applicable)."),
    drug_name: List[str] = typer.Option(None, "--drug-name", help="Name of a drug to search for package inserts. Can be specified multiple times.")
):
    """
    Run an ETL process for a specific dataset defined in the config.
    """
    print(f"Starting ETL run for dataset '{dataset}'.")
    # Move argument validation to the top, before any connections are made.
    if dataset == "approvals" and not year:
        raise ValueError("The '--year' option is required for the 'approvals' dataset.")
    if dataset in ["package_inserts", "review_reports"] and not drug_name:
        raise ValueError(f"At least one '--drug-name' option is required for the '{dataset}' dataset.")

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
        state_schema = schemas.INGESTION_STATE_SCHEMA["schema_name"]
        last_state = adapter.get_latest_state(dataset, schema=state_schema)
        print(f"Last state for '{dataset}': {last_state}")

        # 5. Get ETL Classes from Registry
        extractor_class = AVAILABLE_EXTRACTORS.get(ds_config["extractor"])
        parser_class = AVAILABLE_PARSERS.get(ds_config["parser"])
        transformer_class = AVAILABLE_TRANSFORMERS.get(ds_config["transformer"])
        if not all([extractor_class, parser_class, transformer_class]):
            raise ValueError(f"One or more ETL classes for '{dataset}' could not be found.")

        # 6. Execute ETL - A. Extract
        print(f"--- Running Extractor: {ds_config['extractor']} ---")
        extractor_instance = extractor_class()
        extract_args = {"last_state": last_state}
        if dataset == "approvals":
            extract_args['year'] = year
        elif dataset in ["package_inserts", "review_reports"]:
            extract_args['drug_names'] = drug_name

        extracted_output = extractor_instance.extract(**extract_args)
        new_state = extracted_output[-1]

        # 7. Delta Check
        if new_state == last_state and last_state:
            print("Data source has not changed since last run. Pipeline will stop.")
            status = "SUCCESS"
            adapter.update_state(dataset, state=new_state, status=status, schema=state_schema)
            adapter.commit()
            return

        # 8. Parse, Transform, and Load based on dataset type
        if dataset in ["package_inserts", "review_reports"]:
            downloaded_data, _ = extracted_output

            for file_path, source_url in downloaded_data:
                print(f"\n--- Processing file: {file_path.name} from {source_url} ---")
                parser_instance = parser_class()
                parsed_output = parser_instance.parse(file_path)

                # Parsers for these types now consistently return a list of DataFrames.
                # The check for an empty list is sufficient.
                if not parsed_output:
                    print(f"Parser returned no data for {file_path.name}. Skipping.")
                    continue

                transformer_instance = transformer_class(source_url=source_url)
                transformed_df = transformer_instance.transform(parsed_output)

                load_mode = mode or ds_config.get("load_mode", "merge")
                table_name = ds_config["table_name"]
                schema_name = ds_config["schema_name"]
                primary_keys = ds_config.get("primary_key")

                print(f"--- Loading data for {file_path.name} to {schema_name}.{table_name} (mode: {load_mode}) ---")
                if load_mode == "merge":
                    if not primary_keys:
                        raise ValueError(f"load_mode 'merge' requires 'primary_key' in config for dataset '{dataset}'.")
                    staging_table_name = f"staging_{table_name}"
                    staging_schema = {"schema_name": schema_name, "tables": {staging_table_name: {"columns": target_schema_def["tables"][table_name]["columns"]}}}
                    try:
                        adapter.ensure_schema(staging_schema)
                        adapter.bulk_load(data=transformed_df, target_table=staging_table_name, schema=schema_name, mode="overwrite")
                        adapter.execute_merge(staging_table=staging_table_name, target_table=table_name, primary_keys=primary_keys, schema=schema_name)
                    finally:
                        adapter.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{staging_table_name};")
                else:
                     adapter.bulk_load(data=transformed_df, target_table=table_name, schema=schema_name, mode=load_mode)
            status = "SUCCESS"

        elif dataset in ["approvals", "jader"]:
            file_path, source_url, _ = extracted_output
            print(f"--- Running Parser: {ds_config['parser']} ---")
            parser_instance = parser_class()
            raw_df = parser_instance.parse(file_path)

            print(f"--- Running Transformer: {ds_config['transformer']} ---")
            transformer_instance = transformer_class(source_url=source_url)
            transformed_output = transformer_instance.transform(raw_df)

            load_mode = mode or ds_config.get("load_mode", "overwrite")
            schema_name = ds_config["schema_name"]

            if isinstance(transformed_output, dict):
                print(f"--- Loading multiple tables for dataset '{dataset}' (mode: {load_mode}) ---")
                for table_name, df in transformed_output.items():
                    print(f"Loading data into {schema_name}.{table_name}...")
                    if df.empty:
                        print(f"Transformed DataFrame for table '{table_name}' is empty. Nothing to load.")
                        continue
                    current_load_mode = "overwrite" if load_mode == "merge" else load_mode
                    if load_mode == "merge":
                        print(f"Warning: 'merge' mode is not yet supported for multi-table datasets. Defaulting to 'overwrite' for table {table_name}.")
                    adapter.bulk_load(data=df, target_table=table_name, schema=schema_name, mode=current_load_mode)
            else:
                table_name = ds_config["table_name"]
                print(f"--- Loading data to {schema_name}.{table_name} (mode: {load_mode}) ---")
                if transformed_output.empty:
                    print("Transformed DataFrame is empty. Nothing to load.")
                elif load_mode == "merge":
                    primary_keys = ds_config.get("primary_key")
                    if not primary_keys:
                        raise ValueError(f"load_mode 'merge' requires 'primary_key' in config for dataset '{dataset}'.")
                    staging_table_name = f"staging_{table_name}"
                    staging_schema = {"schema_name": schema_name, "tables": {staging_table_name: {"columns": target_schema_def["tables"][table_name]["columns"]}}}
                    try:
                        adapter.ensure_schema(staging_schema)
                        adapter.bulk_load(data=transformed_output, target_table=staging_table_name, schema=schema_name, mode="overwrite")
                        adapter.execute_merge(staging_table=staging_table_name, target_table=table_name, primary_keys=primary_keys, schema=schema_name)
                    finally:
                        adapter.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{staging_table_name};")
                else:
                    adapter.bulk_load(data=transformed_output, target_table=table_name, schema=schema_name, mode=load_mode)
            status = "SUCCESS"

        # 9. Update State
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
