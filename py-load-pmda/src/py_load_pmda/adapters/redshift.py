import io
import json
import logging
import uuid
from datetime import datetime, timezone
from importlib.metadata import version
from typing import Any, Dict, List, Optional, cast

import boto3
import pandas as pd
import redshift_connector

from py_load_pmda.interfaces import LoaderInterface


class RedshiftAdapter(LoaderInterface):
    """
    Database adapter for Amazon Redshift.

    Implements the LoaderInterface to provide specific logic for connecting to,
    loading data into, and managing state in a Redshift database.
    This adapter uses a staging approach for bulk loads:
    1. Data is uploaded to an S3 bucket as a Parquet file.
    2. A COPY command is executed in Redshift to load the data from S3.
    """
    def __init__(self) -> None:
        self.conn: Optional[redshift_connector.Connection] = None
        self.s3_staging_bucket: Optional[str] = None
        self.iam_role: Optional[str] = None

    def connect(self, connection_details: Dict[str, Any]) -> None:
        """
        Establish connection to the target Redshift database.

        Args:
            connection_details: A dictionary with connection parameters
                                (e.g., host, port, user, password, database).

        Raises:
            ConnectionError: If the database connection fails.
        """
        if self.conn:
            return

        try:
            # Pop S3-specific config to pass the rest to the connector
            connect_params = connection_details.copy()
            self.s3_staging_bucket = connect_params.pop("s3_staging_bucket", None)
            self.iam_role = connect_params.pop("iam_role", None)

            connect_params.pop("type", None)
            # Redshift connector uses 'database' instead of 'dbname'
            if "dbname" in connect_params:
                connect_params["database"] = connect_params.pop("dbname")

            self.conn = redshift_connector.connect(**connect_params)
            logging.info("Successfully connected to Redshift.")
        except redshift_connector.Error as e:
            logging.error(f"Error: Unable to connect to Redshift database: {e}")
            raise ConnectionError("Failed to connect to Redshift.") from e

    def ensure_schema(self, schema_definition: Dict[str, Any]) -> None:
        """
        Ensure the target schema and tables exist in Redshift.
        This method does NOT commit the transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected to the database. Call connect() first.")

        schema_name = schema_definition.get("schema_name")
        if not schema_name:
            raise ValueError("schema_name not provided in schema_definition")

        tables = schema_definition.get("tables", {})

        with self.conn.cursor() as cursor:
            try:
                logging.info(f"Ensuring schema '{schema_name}' exists...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

                for table_name, table_def in tables.items():
                    logging.info(f"Ensuring table '{schema_name}.{table_name}' exists...")
                    columns = table_def.get("columns", {})
                    if not columns:
                        continue

                    # Redshift uses SUPER for JSON, but VARCHAR(MAX) is a safe fallback
                    col_defs = [
                        f"{col_name} {col_type.replace('JSONB', 'SUPER')}"
                        for col_name, col_type in columns.items()
                    ]
                    pk = table_def.get("primary_key")
                    if pk:
                        col_defs.append(f"PRIMARY KEY ({pk})")

                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                        {', '.join(col_defs)}
                    );
                    """
                    cursor.execute(create_table_sql)
                logging.info("Schema and tables verified successfully.")
            except redshift_connector.Error as e:
                logging.error(f"Error during schema creation: {e}")
                if self.conn:
                    self.conn.rollback()
                raise

    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        """
        Perform high-performance native bulk load using a staging S3 bucket.

        This method converts the DataFrame to Parquet, uploads it to S3,
        and then uses the Redshift COPY command to load the data.

        The `connection_details` provided during connect() must contain:
        - 's3_staging_bucket': The S3 bucket to use for staging.
        - 'iam_role': The ARN of the IAM role with S3 access for Redshift.

        Args:
            data: The pandas DataFrame to load.
            target_table: The name of the target table in Redshift.
            schema: The database schema of the target table.
            mode: 'append' or 'overwrite'. 'overwrite' will truncate the table first.

        Raises:
            ConnectionError: If not connected to the database.
            ValueError: If required S3 configuration is missing or mode is invalid.
        """
        if not self.conn:
            raise ConnectionError("Not connected to the database. Call connect() first.")
        if data.empty:
            logging.info("DataFrame is empty, skipping bulk load.")
            return
        if mode not in ["append", "overwrite"]:
            raise ValueError("Mode must be either 'append' or 'overwrite'.")

        if not self.s3_staging_bucket or not self.iam_role:
            raise ValueError("s3_staging_bucket and iam_role must be provided in connection_details")

        s3_client = boto3.client("s3")
        s3_key = f"staging/{schema}_{target_table}_{uuid.uuid4()}.parquet"

        try:
            # Step 4a: Convert DataFrame to Parquet in-memory
            logging.info(f"Converting DataFrame to Parquet for table {target_table}...")
            buffer = io.BytesIO()
            data.to_parquet(buffer, index=False)
            buffer.seek(0)

            # Step 4b: Upload to S3
            logging.info(f"Uploading Parquet file to s3://{self.s3_staging_bucket}/{s3_key}")
            s3_client.upload_fileobj(buffer, self.s3_staging_bucket, s3_key)

            # Step 4c: Execute Redshift COPY command
            with self.conn.cursor() as cursor:
                if mode == "overwrite":
                    truncate_sql = f"TRUNCATE TABLE {schema}.{target_table};"
                    logging.info(f"Overwriting table: executing `{truncate_sql}`")
                    cursor.execute(truncate_sql)

                copy_sql = f"""
                COPY {schema}.{target_table}
                FROM 's3://{self.s3_staging_bucket}/{s3_key}'
                IAM_ROLE '{self.iam_role}'
                FORMAT AS PARQUET;
                """
                logging.info(f"Starting bulk load from S3 for '{schema}.{target_table}'...")
                cursor.execute(copy_sql)
                logging.info(f"Successfully loaded {len(data)} rows.")

        except (s3_client.exceptions.S3UploadFailedError, redshift_connector.Error) as e:
            logging.error(f"Error during bulk load: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            # Step 4d: Cleanup S3
            logging.info(f"Cleaning up S3 object: s3://{self.s3_staging_bucket}/{s3_key}")
            try:
                s3_client.delete_object(Bucket=self.s3_staging_bucket, Key=s3_key)
            except Exception as e:
                logging.error(f"Failed to delete S3 object s3://{self.s3_staging_bucket}/{s3_key}: {e}")


    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: List[str], schema: str
    ) -> None:
        """
        Execute a MERGE (Upsert) operation from a staging table using a
        DELETE and INSERT pattern, which is standard for Redshift.
        This method does NOT commit the transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")
        if not primary_keys:
            raise ValueError("primary_keys must be provided for a merge operation.")

        logging.info(f"Merging data from '{schema}.{staging_table}' to '{schema}.{target_table}'...")

        join_condition = " AND ".join(
            f"t.{pk} = s.{pk}" for pk in primary_keys
        )

        with self.conn.cursor() as cursor:
            try:
                # 1. Delete rows from target that exist in staging
                delete_sql = f"""
                DELETE FROM {schema}.{target_table} t
                USING {schema}.{staging_table} s
                WHERE {join_condition};
                """
                logging.info("Executing DELETE portion of merge...")
                cursor.execute(delete_sql)
                logging.info(f"{cursor.rowcount} rows deleted from target table.")

                # 2. Insert all rows from staging
                insert_sql = f"""
                INSERT INTO {schema}.{target_table}
                SELECT * FROM {schema}.{staging_table};
                """
                logging.info("Executing INSERT portion of merge...")
                cursor.execute(insert_sql)
                logging.info(f"{cursor.rowcount} rows inserted from staging table.")

                logging.info("Successfully merged data.")

            except redshift_connector.Error as e:
                logging.error(f"Error during merge operation: {e}")
                if self.conn:
                    self.conn.rollback()
                raise

    def get_latest_state(self, dataset_id: str, schema: str) -> Dict[str, Any]:
        """
        Retrieve the latest ingestion state for a dataset from Redshift.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        # Note: Redshift does not have native JSONB, assumes SUPER or VARCHAR
        query = f"SELECT last_watermark FROM {schema}.ingestion_state WHERE dataset_id = %s;"
        with self.conn.cursor() as cursor:
            cursor.execute(query, (dataset_id,))
            result = cursor.fetchone()
            if result and result[0]:
                # last_watermark could be a string that needs to be parsed
                if isinstance(result[0], str):
                    return cast(Dict[str, Any], json.loads(result[0]))
                return cast(Dict[str, Any], result[0])
            return {}

    def update_state(self, dataset_id: str, state: Dict[str, Any], status: str, schema: str) -> None:
        """
        Update the ingestion state for a dataset in Redshift.
        This method does NOT commit the transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        pipeline_version = version("py-load-pmda")
        now = datetime.now(timezone.utc)
        last_watermark = json.dumps(state)
        # Redshift does not support TIMESTAMPTZ in the same way, handle None for failed runs
        last_successful_ts = now if status == 'SUCCESS' else None

        # Redshift ON CONFLICT syntax is different, using a merge-like pattern
        # For simplicity, we use a DELETE + INSERT pattern here as well.
        # A more optimized way would be to use a staging table.
        with self.conn.cursor() as cursor:
            try:
                delete_sql = f"DELETE FROM {schema}.ingestion_state WHERE dataset_id = %s;"
                cursor.execute(delete_sql, (dataset_id,))

                insert_sql = f"""
                INSERT INTO {schema}.ingestion_state (
                    dataset_id, last_run_ts_utc, last_successful_run_ts_utc,
                    status, last_watermark, pipeline_version
                )
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_sql, (
                    dataset_id, now, last_successful_ts, status, last_watermark, pipeline_version
                ))
                logging.info(f"State for dataset '{dataset_id}' updated with status '{status}'.")
            except redshift_connector.Error as e:
                logging.error(f"Error updating state for dataset '{dataset_id}': {e}")
                if self.conn:
                    self.conn.rollback()
                raise

    def get_all_states(self, schema: str) -> List[Dict[str, Any]]:
        """Retrieve all ingestion states from the database."""
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        query = f"SELECT * FROM {schema}.ingestion_state ORDER BY dataset_id;"
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            # Manually build list of dicts from column names and rows
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
