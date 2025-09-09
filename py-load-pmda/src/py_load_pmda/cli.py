import logging
from typing import Any, Dict, List, Optional, Type, cast

import typer

from py_load_pmda import extractor, parser, schemas, transformer
from py_load_pmda.adapters.bigquery import BigQueryLoader
from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda.adapters.redshift import RedshiftAdapter
from py_load_pmda.config import load_config
from py_load_pmda.extractor import BaseExtractor
from py_load_pmda.interfaces import LoaderInterface
from py_load_pmda.logging_config import setup_logging

app = typer.Typer()

# --- ETL Class Registries ---
# This allows us to look up the specific ETL class based on a string from the config
AVAILABLE_EXTRACTORS: Dict[str, Type[BaseExtractor]] = {
    "ApprovalsExtractor": extractor.ApprovalsExtractor,
    "JaderExtractor": extractor.JaderExtractor,
    "PackageInsertsExtractor": extractor.PackageInsertsExtractor,
    "ReviewReportsExtractor": extractor.ReviewReportsExtractor,
}

AVAILABLE_PARSERS: Dict[str, Any] = {
    "ApprovalsParser": parser.ApprovalsParser,
    "JaderParser": parser.JaderParser,
    "PackageInsertsParser": parser.PackageInsertsParser,
    "ReviewReportsParser": parser.ReviewReportsParser,
}

AVAILABLE_TRANSFORMERS: Dict[str, Any] = {
    "ApprovalsTransformer": transformer.ApprovalsTransformer,
    "JaderTransformer": transformer.JaderTransformer,
    "PackageInsertsTransformer": transformer.PackageInsertsTransformer,
    "ReviewReportsTransformer": transformer.ReviewReportsTransformer,
}


def get_db_adapter(db_type: str) -> LoaderInterface:
    """Factory function for database adapters."""
    if db_type == "postgres":
        return PostgreSQLAdapter()
    if db_type == "redshift":
        return RedshiftAdapter()
    if db_type == "bigquery":
        return BigQueryLoader()
    raise NotImplementedError(f"Database type '{db_type}' is not supported.")


@app.command()
def init_db() -> None:
    """
    Initialize the database by creating the core schema and state tables.
    """
    config = load_config()
    setup_logging(level=config.get("logging", {}).get("level", "INFO"))

    logging.info("Initializing database...")
    adapter = None
    try:
        db_config = config.get("database", {})
        adapter = get_db_adapter(db_config.get("type", "postgres"))
        adapter.connect(db_config)
        adapter.ensure_schema(schemas.INGESTION_STATE_SCHEMA)
        adapter.commit()
        logging.info("✅ Database initialization complete.")
    except (FileNotFoundError, ConnectionError, ValueError):
        if adapter:
            adapter.rollback()
        logging.error("❌ Database initialization failed", exc_info=True)
        raise typer.Exit(code=1)
    finally:
        if adapter:
            adapter.close()


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

    adapter = None
    status = "FAILED"
    new_state = {}

    try:
        # 1. Load Configuration and Setup Logging
        config = load_config()
        setup_logging(level=config.get("logging", {}).get("level", "INFO"))
        logging.info(f"Starting ETL run for dataset '{dataset}'.")

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
        state_schema = str(schemas.INGESTION_STATE_SCHEMA["schema_name"])
        last_state = adapter.get_latest_state(dataset, schema=state_schema)
        logging.debug(f"Last state for '{dataset}': {last_state}")

        # 5. Get ETL Classes from Registry
        extractor_class = AVAILABLE_EXTRACTORS.get(ds_config["extractor"])
        parser_class = AVAILABLE_PARSERS.get(ds_config["parser"])
        transformer_class = AVAILABLE_TRANSFORMERS.get(ds_config["transformer"])
        if not all([extractor_class, parser_class, transformer_class]):
            raise ValueError(f"One or more ETL classes for '{dataset}' could not be found.")
        extractor_class = cast(Type[BaseExtractor], extractor_class)
        parser_class = cast(Any, parser_class)
        transformer_class = cast(Any, transformer_class)

        # 6. Execute ETL - A. Extract
        logging.info(f"--- Running Extractor: {ds_config['extractor']} ---")
        extractor_instance = extractor_class()
        extract_args: Dict[str, Any] = {"last_state": last_state}
        if dataset == "approvals":
            extract_args['year'] = year
        elif dataset in ["package_inserts", "review_reports"]:
            extract_args['drug_names'] = drug_name

        extracted_output = extractor_instance.extract(**extract_args) # type: ignore
        new_state = extracted_output[-1]

        # 7. Delta Check
        if new_state == last_state and last_state:
            logging.info("Data source has not changed since last run. Pipeline will stop.")
            status = "SUCCESS"
            adapter.update_state(dataset, state=new_state, status=status, schema=state_schema)
            adapter.commit()
            return

        # 8. Parse, Transform, and Load based on dataset type
        if dataset in ["package_inserts", "review_reports"]:
            downloaded_data, _ = cast(Any, extracted_output)

            for file_path, source_url in downloaded_data:
                logging.info(f"--- Processing file: {file_path.name} from {source_url} ---")
                parser_instance = parser_class()
                parsed_output = parser_instance.parse(file_path)

                # The PDF parsers now return a tuple of (full_text, tables).
                if not parsed_output or (not parsed_output[0] and not parsed_output[1]):
                    logging.warning(f"Parser returned no text or tables for {file_path.name}. Skipping.")
                    continue

                transformer_instance = transformer_class(source_url=source_url)
                # The transformer expects the full (text, tables) tuple.
                transformed_df = transformer_instance.transform(parsed_output)

                load_mode = mode or ds_config.get("load_mode", "merge")
                table_name = ds_config["table_name"]
                schema_name = ds_config["schema_name"]
                primary_keys = ds_config.get("primary_key")

                logging.info(f"--- Loading data for {file_path.name} to {schema_name}.{table_name} (mode: {load_mode}) ---")
                if load_mode == "merge":
                    primary_keys = ds_config.get("primary_key")
                    if not primary_keys:
                        raise ValueError(f"load_mode 'merge' requires 'primary_key' in config for dataset '{dataset}'.")
                    staging_table_name = f"staging_{str(table_name)}"
                    if not target_schema_def or not target_schema_def.get("tables") or not cast(Dict[str, Any], target_schema_def.get("tables")).get(str(table_name)):
                        raise ValueError(f"Schema definition for table '{table_name}' not found.")
                    tables = cast(Dict[str, Any], target_schema_def.get("tables"))
                    table_def = cast(Dict[str, Any], tables.get(str(table_name)))
                    staging_schema = {"schema_name": schema_name, "tables": {staging_table_name: {"columns": table_def["columns"]}}}
                    try:
                        adapter.ensure_schema(staging_schema)
                        adapter.bulk_load(data=transformed_df, target_table=staging_table_name, schema=str(schema_name), mode="overwrite")
                        adapter.execute_merge(staging_table=staging_table_name, target_table=str(table_name), primary_keys=cast(List[str], primary_keys), schema=str(schema_name))
                    finally:
                        adapter.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{staging_table_name};")
                else:
                     adapter.bulk_load(data=transformed_df, target_table=str(table_name), schema=str(schema_name), mode=load_mode)
            status = "SUCCESS"

        elif dataset in ["approvals", "jader"]:
            file_path, source_url, _ = cast(Any, extracted_output)
            logging.info(f"--- Running Parser: {ds_config['parser']} ---")
            parser_instance = parser_class()
            raw_df = parser_instance.parse(file_path)

            logging.info(f"--- Running Transformer: {ds_config['transformer']} ---")
            transformer_instance = transformer_class(source_url=source_url)
            transformed_output = transformer_instance.transform(raw_df)

            load_mode = str(mode or ds_config.get("load_mode", "overwrite"))
            schema_name = str(ds_config["schema_name"])

            if isinstance(transformed_output, dict):
                logging.info(f"--- Loading multiple tables for dataset '{dataset}' (mode: {load_mode}) ---")
                for table_name, df in transformed_output.items():
                    logging.info(f"Loading data into {schema_name}.{table_name}...")
                    if df.empty:
                        logging.info(f"Transformed DataFrame for table '{table_name}' is empty. Nothing to load.")
                        continue

                    if load_mode == "merge":
                        # Get primary key from the new config structure for multi-table datasets
                        table_config = ds_config.get("tables", {}).get(table_name, {})
                        primary_keys = table_config.get("primary_key")
                        if not primary_keys:
                            raise ValueError(f"load_mode 'merge' requires 'primary_key' in config for table '{table_name}'.")

                        staging_table_name = f"staging_{table_name}"
                        # Get the column definitions for the specific table from the schema
                        if not target_schema_def or not target_schema_def.get("tables") or not cast(Dict[str, Any], target_schema_def.get("tables")).get(table_name):
                            raise ValueError(f"Schema definition for table '{table_name}' not found.")
                        tables = cast(Dict[str, Any], target_schema_def.get("tables"))
                        table_def = cast(Dict[str, Any], tables.get(table_name))
                        staging_schema = {"schema_name": schema_name, "tables": {staging_table_name: {"columns": table_def["columns"]}}}

                        try:
                            adapter.ensure_schema(staging_schema)
                            adapter.bulk_load(data=df, target_table=staging_table_name, schema=schema_name, mode="overwrite")
                            adapter.execute_merge(staging_table=staging_table_name, target_table=table_name, primary_keys=primary_keys, schema=schema_name)
                        finally:
                            adapter.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{staging_table_name};")
                    else:
                        # Fallback to existing append/overwrite logic
                        adapter.bulk_load(data=df, target_table=table_name, schema=schema_name, mode=load_mode)
            else:
                table_name = str(ds_config["table_name"])
                logging.info(f"--- Loading data to {schema_name}.{table_name} (mode: {load_mode}) ---")
                if transformed_output.empty:
                    logging.info("Transformed DataFrame is empty. Nothing to load.")
                elif load_mode == "merge":
                    primary_keys = ds_config.get("primary_key")
                    if not primary_keys:
                        raise ValueError(f"load_mode 'merge' requires 'primary_key' in config for dataset '{dataset}'.")
                    staging_table_name = f"staging_{table_name}"
                    if not target_schema_def or not target_schema_def.get("tables") or not cast(Dict[str, Any], target_schema_def.get("tables")).get(table_name):
                        raise ValueError(f"Schema definition for table '{table_name}' not found.")
                    tables = cast(Dict[str, Any], target_schema_def.get("tables"))
                    table_def = cast(Dict[str, Any], tables.get(table_name))
                    staging_schema = {"schema_name": schema_name, "tables": {staging_table_name: {"columns": table_def["columns"]}}}
                    try:
                        adapter.ensure_schema(staging_schema)
                        adapter.bulk_load(data=transformed_output, target_table=staging_table_name, schema=schema_name, mode="overwrite")
                        adapter.execute_merge(staging_table=staging_table_name, target_table=table_name, primary_keys=cast(List[str], primary_keys), schema=schema_name)
                    finally:
                        adapter.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{staging_table_name};")
                else:
                    adapter.bulk_load(data=transformed_output, target_table=table_name, schema=schema_name, mode=load_mode)
            status = "SUCCESS"

        # 9. Update State
        logging.info(f"✅ ETL run for dataset '{dataset}' completed successfully.")

    except Exception:
        logging.error(f"❌ ETL run failed for dataset '{dataset}'", exc_info=True)
        if adapter:
            adapter.rollback()
        raise typer.Exit(code=1)

    finally:
        if adapter:
            # Always update the state, whether success or failure
            adapter.update_state(dataset, state=new_state, status=status, schema=str(schemas.INGESTION_STATE_SCHEMA["schema_name"]))
            adapter.commit()
            adapter.close()


@app.command()
def status() -> None:
    """
    Check the status of the last runs from the ingestion_state table.
    """
    config = load_config()
    setup_logging(level=config.get("logging", {}).get("level", "INFO"))

    logging.info("--- Ingestion Status ---")
    adapter = None
    try:
        db_config = config.get("database", {})
        adapter = get_db_adapter(str(db_config.get("type", "postgres")))
        adapter.connect(db_config)
        states = adapter.get_all_states(schema=str(schemas.INGESTION_STATE_SCHEMA["schema_name"]))

        if not states:
            logging.info("No ingestion state found in the database.")
            return

        import pandas as pd
        df = pd.DataFrame(states)
        for col in ['last_run_ts_utc', 'last_successful_run_ts_utc']:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.drop(columns=['last_watermark'], inplace=True, errors='ignore')
        # For the status command, printing the dataframe is the desired output,
        # so we log it directly. The JSON formatter will handle the structure.
        logging.info(df.to_string())

    except Exception:
        logging.error("❌ Failed to get status", exc_info=True)
        raise typer.Exit(code=1)
    finally:
        if adapter:
            adapter.close()

@app.command()
def check_config() -> None:
    """
    Validate configuration and database connectivity.
    """
    config = load_config()
    setup_logging(level=config.get("logging", {}).get("level", "INFO"))

    logging.info("Checking configuration...")
    adapter = None
    try:
        logging.info("✅ Configuration file loaded successfully.")
        db_config = config.get("database", {})
        adapter_type = db_config.get("type")
        if not adapter_type:
            raise ValueError("Database 'type' not specified in config.")

        logging.info(f"▶️ Database adapter type: {adapter_type}")
        adapter = get_db_adapter(adapter_type)
        logging.info("▶️ Attempting to connect to the database...")
        adapter.connect(db_config)
        adapter.commit() # Test transaction
        logging.info("✅ Configuration check passed. Database connection successful.")
    except (FileNotFoundError, ConnectionError, ValueError, NotImplementedError):
        if adapter:
            adapter.rollback()
        logging.error("❌ Configuration check failed", exc_info=True)
        raise typer.Exit(code=1)
    finally:
        if adapter:
            adapter.close()


if __name__ == "__main__":
    app()
