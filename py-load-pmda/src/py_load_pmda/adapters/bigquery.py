import json
import logging
import uuid
from io import BytesIO
from typing import Any, Dict, List

import pandas as pd
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery, storage

from py_load_pmda.interfaces import LoaderInterface

# A mapping from pandas/Python types to BigQuery data types
# A mapping from abstract or pandas types to BigQuery data types
BIGQUERY_TYPE_MAP = {
    # Abstract types from config
    "JSONB": "JSON",
    "JSON": "JSON",
    "TEXT": "STRING",
    "VARCHAR": "STRING",  # Assuming VARCHAR can be mapped to STRING
    # Pandas dtypes
    "object": "STRING",
    "int64": "INT64",
    "float64": "FLOAT64",
    "bool": "BOOLEAN",
    "datetime64[ns]": "DATETIME",
    "datetime64[ns, UTC]": "TIMESTAMP",
    "date": "DATE",
}


STATE_TABLE_NAME = "_ingestion_state"


class BigQueryAdapter(LoaderInterface):
    """
    LoaderInterface implementation for Google BigQuery.
    """

    def __init__(self) -> None:
        self.client: bigquery.Client | None = None
        self.gcs_client: storage.Client | None = None
        self.project_id: str | None = None
        self.gcs_bucket_name: str | None = None
        self.location: str | None = None

    def connect(self, connection_details: Dict[str, Any]) -> None:
        """Establish connection to BigQuery and GCS."""
        self.project_id = connection_details.get("project")
        self.gcs_bucket_name = connection_details.get("gcs_bucket")
        self.location = connection_details.get("location", "US")

        if not self.project_id:
            raise ValueError("BigQuery 'project' must be specified in connection details.")
        if not self.gcs_bucket_name:
            raise ValueError("BigQuery 'gcs_bucket' must be specified in connection details.")

        try:
            self.client = bigquery.Client(project=self.project_id)
            self.gcs_client = storage.Client(project=self.project_id)
            logging.info(f"Successfully connected to BigQuery project: {self.project_id}")
        except Exception as e:
            logging.error(f"Failed to connect to Google Cloud: {e}")
            raise

    def disconnect(self) -> None:
        """Disconnect is a no-op for BigQuery as connections are stateless."""
        self.client = None
        self.gcs_client = None
        logging.info("BigQuery and GCS clients have been cleared.")

    def commit(self) -> None:
        """Commit is a no-op for BigQuery as most operations are auto-committed."""
        pass

    def rollback(self) -> None:
        """Rollback is a no-op for BigQuery."""
        pass

    def execute_sql(self, query: str, params: Any = None) -> None:
        """Executes an arbitrary SQL command in BigQuery."""
        if not self.client:
            raise ConnectionError("Not connected. Call connect() first.")
        # Note: BigQuery's Python client uses named parameters in the query string
        # and a different parameter format than psycopg2. This is a simple pass-through.
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        self.client.query(query, job_config=job_config).result()

    def _get_bq_schema(self, columns: Dict[str, Any]) -> List[bigquery.SchemaField]:
        """Converts a dictionary of column names and types to a BigQuery schema."""
        bq_schema = []
        for col_name, col_type in columns.items():
            # col_type can be a string from YAML (e.g., "JSONB") or a pandas dtype
            col_type_str = str(col_type).upper()

            # Handle complex types like VARCHAR(100)
            if 'VARCHAR' in col_type_str:
                bq_type = BIGQUERY_TYPE_MAP.get("VARCHAR", "STRING")
            else:
                bq_type = BIGQUERY_TYPE_MAP.get(col_type_str, None)

            # Fallback for pandas dtypes if no direct match
            if bq_type is None:
                bq_type = BIGQUERY_TYPE_MAP.get(str(col_type), "STRING")

            bq_schema.append(bigquery.SchemaField(col_name, bq_type))
        return bq_schema

    def ensure_schema(self, schema_definition: Dict[str, Any]) -> None:
        """Ensure the target BigQuery dataset and tables exist."""
        if not self.client:
            raise ConnectionError("Not connected to BigQuery. Call connect() first.")

        dataset_name = schema_definition.get("name")
        if not dataset_name:
            raise ValueError("Schema definition must include a 'name' for the dataset.")
        dataset_id = f"{self.project_id}.{dataset_name}"

        try:
            self.client.get_dataset(dataset_id)
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.location
            self.client.create_dataset(dataset, timeout=30)

        # Ensure state table exists
        state_table_id = f"{dataset_id}.{STATE_TABLE_NAME}"
        try:
            self.client.get_table(state_table_id)
        except NotFound:
            state_schema = [
                bigquery.SchemaField("dataset_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_run_ts_utc", "TIMESTAMP"),
                bigquery.SchemaField("last_successful_run_ts_utc", "TIMESTAMP"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("last_watermark", "JSON"),
                bigquery.SchemaField("pipeline_version", "STRING"),
            ]
            table = bigquery.Table(state_table_id, schema=state_schema)
            self.client.create_table(table)

        for table_name, table_details in schema_definition.get("tables", {}).items():
            table_id = f"{dataset_id}.{table_name}"
            try:
                self.client.get_table(table_id)
            except NotFound:
                columns = table_details.get("columns", {})
                bq_schema = self._get_bq_schema(columns)
                table = bigquery.Table(table_id, schema=bq_schema)
                self.client.create_table(table)

    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        """Perform high-performance native bulk load of the data via GCS."""
        if not self.client or not self.gcs_client or not self.gcs_bucket_name:
            raise ConnectionError("Not connected. Call connect() first.")

        table_id = f"{self.project_id}.{schema}.{target_table}"
        if mode == "overwrite":
            self.client.delete_table(table_id, not_found_ok=True)
            bq_schema = self._get_bq_schema(data.dtypes.to_dict())
            table = bigquery.Table(table_id, schema=bq_schema)
            self.client.create_table(table)

        parquet_buffer = BytesIO()
        data.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        bucket = self.gcs_client.bucket(self.gcs_bucket_name)
        blob_name = f"staging/{target_table}_{uuid.uuid4()}.parquet"
        blob = bucket.blob(blob_name)
        blob.upload_from_file(parquet_buffer)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            if mode == "append"
            else bigquery.WriteDisposition.WRITE_EMPTY,
        )
        load_job = self.client.load_table_from_uri(
            f"gs://{self.gcs_bucket_name}/{blob_name}", table_id, job_config=job_config
        )
        load_job.result()
        blob.delete()

    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: List[str], schema: str
    ) -> None:
        """Execute a MERGE (Upsert) operation in BigQuery."""
        if not self.client:
            raise ConnectionError("Not connected. Call connect() first.")

        target_id = f"{self.project_id}.{schema}.{target_table}"
        staging_id = f"{self.project_id}.{schema}.{staging_table}"

        # Get columns from staging table to build the MERGE statement
        staging_ref = self.client.get_table(staging_id)
        columns = [field.name for field in staging_ref.schema]

        on_clause = " AND ".join([f"T.{pk} = S.{pk}" for pk in primary_keys])
        update_clause = ", ".join([f"T.{col} = S.{col}" for col in columns])
        insert_clause = f"({', '.join(columns)}) VALUES ({', '.join([f'S.{col}' for col in columns])})"

        query = f"""
            MERGE {target_id} T
            USING {staging_id} S
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT {insert_clause}
        """
        self.client.query(query).result()

    def get_latest_state(self, dataset_id: str, schema: str) -> Dict[str, Any]:
        """Retrieve the latest ingestion state for a dataset from BigQuery."""
        if not self.client:
            raise ConnectionError("Not connected. Call connect() first.")

        state_table_id = f"{self.project_id}.{schema}.{STATE_TABLE_NAME}"
        query = f"SELECT * FROM `{state_table_id}` WHERE dataset_id = @dataset_id LIMIT 1"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id)]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            rows = list(query_job.result())
            if not rows:
                return {}

            row_dict = dict(rows[0].items())
            if 'last_watermark' in row_dict and row_dict['last_watermark']:
                row_dict['last_watermark'] = json.loads(row_dict['last_watermark'])
            return row_dict
        except NotFound:
            return {}

    def update_state(self, dataset_id: str, state: Dict[str, Any], status: str, schema: str) -> None:
        """Transactionally update the ingestion state after a load in BigQuery."""
        if not self.client:
            raise ConnectionError("Not connected. Call connect() first.")

        state_table_id = f"{self.project_id}.{schema}.{STATE_TABLE_NAME}"

        # BigQuery's MERGE statement is atomic and can handle the upsert logic
        query = f"""
            MERGE `{state_table_id}` T
            USING (SELECT @dataset_id as dataset_id) S
            ON T.dataset_id = S.dataset_id
            WHEN MATCHED THEN
                UPDATE SET
                    last_run_ts_utc = @last_run,
                    last_successful_run_ts_utc = IF(@status = 'SUCCESS', @last_run, T.last_successful_run_ts_utc),
                    status = @status,
                    last_watermark = @watermark,
                    pipeline_version = @version
            WHEN NOT MATCHED THEN
                INSERT (dataset_id, last_run_ts_utc, last_successful_run_ts_utc, status, last_watermark, pipeline_version)
                VALUES (@dataset_id, @last_run, IF(@status = 'SUCCESS', @last_run, NULL), @status, @watermark, @version)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
                bigquery.ScalarQueryParameter("last_run", "TIMESTAMP", pd.Timestamp.utcnow()),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("watermark", "JSON", json.dumps(state.get("last_watermark", {}))),
                bigquery.ScalarQueryParameter("version", "STRING", state.get("pipeline_version")),
            ]
        )
        self.client.query(query, job_config=job_config).result()


    def get_all_states(self, schema: str) -> List[Dict[str, Any]]:
        """Retrieve all ingestion states from the database."""
        if not self.client:
            raise ConnectionError("Not connected. Call connect() first.")

        state_table_id = f"{self.project_id}.{schema}.{STATE_TABLE_NAME}"
        query = f"SELECT * FROM `{state_table_id}`"

        try:
            rows = self.client.query(query).result()
            results = []
            for row in rows:
                row_dict = dict(row.items())
                if 'last_watermark' in row_dict and row_dict['last_watermark']:
                    row_dict['last_watermark'] = json.loads(row_dict['last_watermark'])
                results.append(row_dict)
            return results
        except NotFound:
            return []
