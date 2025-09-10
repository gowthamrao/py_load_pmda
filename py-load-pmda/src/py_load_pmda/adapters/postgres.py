import io
import json
import logging
from datetime import datetime, timezone
from importlib.metadata import version
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import connection

from py_load_pmda.interfaces import LoaderInterface


class PostgreSQLAdapter(LoaderInterface):
    """
    Database adapter for PostgreSQL.

    Implements the LoaderInterface to provide specific logic for connecting to,
    loading data into, and managing state in a PostgreSQL database.
    """

    def __init__(self) -> None:
        self.conn: Optional[connection] = None

    def connect(self, connection_details: Dict[str, Any]) -> None:
        """Establish connection to the target PostgreSQL database."""
        if self.conn:
            return

        try:
            connect_params = connection_details.copy()
            connect_params.pop("type", None)
            self.conn = psycopg2.connect(**connect_params)
            logging.info("Successfully connected to PostgreSQL.")
        except psycopg2.Error as e:
            logging.error(f"Error: Unable to connect to PostgreSQL database: {e}")
            raise ConnectionError("Failed to connect to PostgreSQL.") from e

    def disconnect(self) -> None:
        """Disconnect from the target PostgreSQL database."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logging.info("PostgreSQL connection closed.")

    def commit(self) -> None:
        """Commit the current database transaction."""
        if not self.conn:
            raise ConnectionError("Not connected to the database.")
        self.conn.commit()

    def rollback(self) -> None:
        """Roll back the current database transaction."""
        if not self.conn:
            raise ConnectionError("Not connected to the database.")
        self.conn.rollback()

    def ensure_schema(self, schema_definition: Dict[str, Any]) -> None:
        """
        Ensure the target schema and tables exist in PostgreSQL.
        This method should be executed within a transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected to the database. Call connect() first.")

        schema_name = schema_definition.get("schema_name")
        if not schema_name:
            raise ValueError("schema_name not provided in schema_definition")

        tables = schema_definition.get("tables", {})

        with self.conn.cursor() as cursor:
            logging.info(f"Ensuring schema '{schema_name}' exists...")
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))

            for table_name, table_def in tables.items():
                logging.info(f"Ensuring table '{schema_name}.{table_name}' exists...")
                columns = table_def.get("columns", {})
                if not columns:
                    continue

                col_defs = [
                    sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
                    for col_name, col_type in columns.items()
                ]
                pk = table_def.get("primary_key")
                if pk:
                    col_defs.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(pk)))

                create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(col_defs),
                )
                cursor.execute(create_table_sql)
            logging.info("Schema and tables verified successfully.")

    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        """
        Perform high-performance native bulk load using COPY.
        This method should be executed within a transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected to the database. Call connect() first.")
        if data.empty:
            logging.info("DataFrame is empty, skipping bulk load.")
            return

        if mode not in ["append", "overwrite"]:
            raise ValueError("Mode must be either 'append' or 'overwrite'.")

        full_table_name = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(target_table))

        with self.conn.cursor() as cursor:
            if mode == "overwrite":
                logging.info(f"Overwriting table: {full_table_name.as_string(cursor)}")
                cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY").format(full_table_name))

            buffer = io.StringIO()
            # Use the CSV format, which is more robust for complex string data.
            # QUOTE_MINIMAL ensures that fields are only quoted if they contain
            # the delimiter, quotechar, or lineterminator.
            data.to_csv(buffer, index=False, header=False, sep=',', na_rep='', quoting=1) # 1 = csv.QUOTE_MINIMAL
            buffer.seek(0)

            # Use FORMAT csv, which correctly handles quoted fields.
            copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT csv, HEADER false)").format(full_table_name)

            logging.info(f"Starting bulk load to '{full_table_name.as_string(cursor)}'...")
            cursor.copy_expert(sql=copy_sql.as_string(cursor), file=buffer)
            logging.info(f"Successfully loaded {len(data)} rows.")

    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: List[str], schema: str
    ) -> None:
        """
        Execute a MERGE (Upsert) operation from a staging table.
        This method should be executed within a transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")
        if not primary_keys:
            raise ValueError("primary_keys must be provided for a merge operation.")

        logging.info(f"Merging data from '{schema}.{staging_table}' to '{schema}.{target_table}'...")

        with self.conn.cursor() as cursor:
            query = sql.SQL("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s;
            """)
            cursor.execute(query, (schema, staging_table))
            table_cols = [row[0] for row in cursor.fetchall()]

            if not table_cols:
                logging.warning(f"Staging table '{schema}.{staging_table}' is empty or does not exist. Skipping merge.")
                return

            update_cols = [col for col in table_cols if col not in primary_keys]
            if not update_cols:
                raise ValueError("No columns to update (all columns are primary keys).")

            update_clause = sql.SQL(", ").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col)) for col in update_cols
            )
            merge_sql = sql.SQL("""
                INSERT INTO {target} ({cols})
                SELECT {cols} FROM {staging}
                ON CONFLICT ({pks}) DO UPDATE SET {update_clause};
            """).format(
                target=sql.Identifier(schema, target_table),
                cols=sql.SQL(", ").join(map(sql.Identifier, table_cols)),
                staging=sql.Identifier(schema, staging_table),
                pks=sql.SQL(", ").join(map(sql.Identifier, primary_keys)),
                update_clause=update_clause,
            )
            cursor.execute(merge_sql)
            logging.info(f"Successfully merged. {cursor.rowcount} rows affected.")

    def get_latest_state(self, dataset_id: str, schema: str) -> Dict[str, Any]:
        """Retrieve the latest ingestion state for a dataset from PostgreSQL."""
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        query = sql.SQL("SELECT * FROM {}.ingestion_state WHERE dataset_id = %s").format(sql.Identifier(schema))
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, (dataset_id,))
            result = cursor.fetchone()
            return dict(result) if result else {}

    def update_state(self, dataset_id: str, state: Dict[str, Any], status: str, schema: str) -> None:
        """
        Update the ingestion state for a dataset.
        This method should be executed within a transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        pipeline_version = version("py_load_pmda")
        now = datetime.now(timezone.utc)
        last_watermark = json.dumps(state.get("last_watermark", {}))

        update_sql = sql.SQL("""
        INSERT INTO {schema}.ingestion_state (
            dataset_id, last_run_ts_utc, last_successful_run_ts_utc,
            status, last_watermark, pipeline_version
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset_id) DO UPDATE SET
            last_run_ts_utc = EXCLUDED.last_run_ts_utc,
            last_successful_run_ts_utc = CASE
                WHEN EXCLUDED.status = 'SUCCESS'
                THEN EXCLUDED.last_successful_run_ts_utc
                ELSE {schema}.ingestion_state.last_successful_run_ts_utc
            END,
            status = EXCLUDED.status,
            last_watermark = EXCLUDED.last_watermark,
            pipeline_version = EXCLUDED.pipeline_version;
        """).format(schema=sql.Identifier(schema))

        with self.conn.cursor() as cursor:
            cursor.execute(update_sql, (
                dataset_id, now, now if status == 'SUCCESS' else None,
                status, last_watermark, pipeline_version
            ))
            logging.info(f"State for dataset '{dataset_id}' updated with status '{status}'.")

    def get_all_states(self, schema: str) -> List[Dict[str, Any]]:
        """Retrieve all ingestion states from the database."""
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        query = sql.SQL("SELECT * FROM {}.ingestion_state ORDER BY dataset_id").format(sql.Identifier(schema))
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    def execute_sql(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        """
        Executes an arbitrary SQL command.
        This method should be executed within a transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected to the database.")
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
