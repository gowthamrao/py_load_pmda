import logging
from typing import Any, cast, Dict, List, Optional, Type

from py_load_pmda import extractor, parser, schemas, transformer
from py_load_pmda.adapters.bigquery import BigQueryLoader
from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda.adapters.redshift import RedshiftAdapter
from py_load_pmda.extractor import BaseExtractor
from py_load_pmda.interfaces import LoaderInterface
from py_load_pmda.logging_config import setup_logging

# --- ETL Class Registries ---
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


class Orchestrator:
    """
    Orchestrates the entire ETL process for a given dataset.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        dataset: str,
        mode: Optional[str] = None,
        year: Optional[int] = None,
        drug_name: Optional[List[str]] = None,
    ) -> None:
        self.config = config
        self.dataset = dataset
        self.mode = mode
        self.year = year
        self.drug_name = drug_name
        self.adapter: Optional[LoaderInterface] = None

        logging_config = self.config.get("logging", {})
        setup_logging(
            level=logging_config.get("level", "INFO"),
            log_format=logging_config.get("format", "text"),
        )
        logging.info(f"Orchestrator initialized for dataset '{self.dataset}'.")

    def run(self) -> None:
        """
        Executes the main ETL pipeline.
        """
        status = "FAILED"
        new_state: Dict[str, Any] = {}
        try:
            logging.info(f"Starting ETL run for dataset '{self.dataset}'.")

            db_config = self.config.get("database", {})
            dataset_configs = self.config.get("datasets", {})
            if self.dataset not in dataset_configs:
                raise ValueError(f"Dataset '{self.dataset}' not found in config.yaml.")
            ds_config = dataset_configs[self.dataset]

            self.adapter = get_db_adapter(db_config.get("type", "postgres"))
            self.adapter.connect(db_config)

            target_schema_def = schemas.DATASET_SCHEMAS.get(self.dataset)
            if not target_schema_def:
                raise ValueError(f"Schema for dataset '{self.dataset}' not found in schemas.py.")
            self.adapter.ensure_schema(target_schema_def)

            state_schema = str(schemas.INGESTION_STATE_SCHEMA["schema_name"])
            last_state = self.adapter.get_latest_state(self.dataset, schema=state_schema)
            logging.debug(f"Last state for '{self.dataset}': {last_state}")

            extractor_class = AVAILABLE_EXTRACTORS.get(ds_config["extractor"])
            parser_class = AVAILABLE_PARSERS.get(ds_config["parser"])
            transformer_class = AVAILABLE_TRANSFORMERS.get(ds_config["transformer"])
            if not all([extractor_class, parser_class, transformer_class]):
                raise ValueError(f"One or more ETL classes for '{self.dataset}' could not be found.")
            extractor_class = cast(Type[BaseExtractor], extractor_class)
            parser_class = cast(Any, parser_class)
            transformer_class = cast(Any, transformer_class)

            logging.info(f"--- Running Extractor: {ds_config['extractor']} ---")
            extractor_instance = extractor_class()
            extract_args: Dict[str, Any] = {"last_state": last_state}
            if self.dataset == "approvals":
                extract_args['year'] = self.year
            elif self.dataset in ["package_inserts", "review_reports"]:
                extract_args['drug_names'] = self.drug_name

            extracted_output = extractor_instance.extract(**extract_args)
            new_state = extracted_output[-1]

            if new_state == last_state and last_state:
                logging.info("Data source has not changed since last run. Pipeline will stop.")
                status = "SUCCESS"
                if self.adapter:
                    self.adapter.update_state(self.dataset, state=new_state, status=status, schema=state_schema)
                    self.adapter.commit()
                return

            if self.dataset in ["package_inserts", "review_reports"]:
                downloaded_data, _ = cast(Any, extracted_output)
                for file_path, source_url in downloaded_data:
                    logging.info(f"--- Processing file: {file_path.name} from {source_url} ---")
                    parser_instance = parser_class()
                    parsed_output = parser_instance.parse(file_path)
                    if not parsed_output or (not parsed_output[0] and not parsed_output[1]):
                        logging.warning(f"Parser returned no text or tables for {file_path.name}. Skipping.")
                        continue
                    transformer_instance = transformer_class(source_url=source_url)
                    transformed_df = transformer_instance.transform(parsed_output)
                    self._load_data(ds_config, target_schema_def, transformed_df)
                status = "SUCCESS"

            elif self.dataset in ["approvals", "jader"]:
                file_path, source_url, _ = cast(Any, extracted_output)
                logging.info(f"--- Running Parser: {ds_config['parser']} ---")
                parser_instance = parser_class()
                raw_df = parser_instance.parse(file_path)
                logging.info(f"--- Running Transformer: {ds_config['transformer']} ---")
                transformer_instance = transformer_class(source_url=source_url)
                transformed_output = transformer_instance.transform(raw_df)
                self._load_data(ds_config, target_schema_def, transformed_output)
                status = "SUCCESS"

            logging.info(f"✅ ETL run for dataset '{self.dataset}' completed successfully.")

        except Exception:
            logging.error(f"❌ ETL run failed for dataset '{self.dataset}'", exc_info=True)
            if self.adapter:
                self.adapter.rollback()
            raise  # Re-raise the exception to be handled by the CLI

        finally:
            if self.adapter:
                if status == "SUCCESS":
                    self.adapter.update_state(self.dataset, state=new_state, status=status, schema=str(schemas.INGESTION_STATE_SCHEMA["schema_name"]))
                    self.adapter.commit()
                self.adapter.close()

    def _load_data(self, ds_config, target_schema_def, data) -> None:
        """Helper method to handle loading data for single or multiple tables."""
        if self.adapter is None:
            raise RuntimeError("Database adapter is not initialized.")

        load_mode = str(self.mode or ds_config.get("load_mode", "overwrite"))
        schema_name = str(ds_config["schema_name"])

        if isinstance(data, dict):
            logging.info(f"--- Loading multiple tables for dataset '{self.dataset}' (mode: {load_mode}) ---")
            for table_name, df in data.items():
                if df.empty:
                    logging.info(f"DataFrame for table '{table_name}' is empty. Skipping.")
                    continue
                self._load_table(ds_config, target_schema_def, table_name, df, load_mode, schema_name)
        else:
            table_name = str(ds_config["table_name"])
            if data.empty:
                logging.info(f"DataFrame for table '{table_name}' is empty. Skipping.")
                return
            self._load_table(ds_config, target_schema_def, table_name, data, load_mode, schema_name)

    def _load_table(self, ds_config, target_schema_def, table_name, df, load_mode, schema_name) -> None:
        """Helper method to load a single DataFrame to a table."""
        if self.adapter is None:
            raise RuntimeError("Database adapter is not initialized.")

        logging.info(f"--- Loading data to {schema_name}.{table_name} (mode: {load_mode}) ---")
        if load_mode == "merge":
            table_config = ds_config.get("tables", {}).get(table_name, ds_config)
            primary_keys = table_config.get("primary_key")
            if not primary_keys:
                raise ValueError(f"load_mode 'merge' requires 'primary_key' in config for table '{table_name}'.")

            staging_table_name = f"staging_{table_name}"
            tables = cast(Dict[str, Any], target_schema_def.get("tables"))
            table_def = cast(Dict[str, Any], tables.get(table_name))
            if not table_def:
                raise ValueError(f"Schema definition for table '{table_name}' not found.")
            staging_schema = {"schema_name": schema_name, "tables": {staging_table_name: {"columns": table_def["columns"]}}}

            try:
                self.adapter.ensure_schema(staging_schema)
                self.adapter.bulk_load(data=df, target_table=staging_table_name, schema=schema_name, mode="overwrite")
                self.adapter.execute_merge(staging_table=staging_table_name, target_table=table_name, primary_keys=primary_keys, schema=schema_name)
            finally:
                self.adapter.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{staging_table_name};")
        else:
            self.adapter.bulk_load(data=df, target_table=table_name, schema=schema_name, mode=load_mode)
