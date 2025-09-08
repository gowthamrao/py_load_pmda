import io
import json
from datetime import datetime, timezone
from importlib.metadata import version
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from py_load_pmda.interfaces import LoaderInterface


class PostgreSQLAdapter(LoaderInterface):
    """
    Database adapter for PostgreSQL.

    Implements the LoaderInterface to provide specific logic for connecting to,
    loading data into, and managing state in a PostgreSQL database.
    """

    def __init__(self):
        self.conn = None

    def connect(self, connection_details: dict) -> None:
        """
        Establish connection to the target PostgreSQL database.

        Args:
            connection_details: A dictionary with connection parameters
                                (e.g., host, port, user, password, dbname).

        Raises:
            ConnectionError: If the database connection fails.
        """
        if self.conn:
            return

        try:
            # The 'type' key is not a valid psycopg2 connection parameter
            connect_params = connection_details.copy()
            connect_params.pop("type", None)

            self.conn = psycopg2.connect(**connect_params)
            print("Successfully connected to PostgreSQL.")
        except psycopg2.Error as e:
            print(f"Error: Unable to connect to PostgreSQL database: {e}")
            raise ConnectionError("Failed to connect to PostgreSQL.") from e

    def commit(self) -> None:
        """Commits the current database transaction."""
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        """Rolls back the current database transaction."""
        if self.conn:
            self.conn.rollback()

    def close(self) -> None:
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("PostgreSQL connection closed.")

    def ensure_schema(self, schema_definition: dict) -> None:
        """
        Ensure the target schema and tables exist in PostgreSQL.
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
                print(f"Ensuring schema '{schema_name}' exists...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

                for table_name, table_def in tables.items():
                    print(f"Ensuring table '{schema_name}.{table_name}' exists...")
                    columns = table_def.get("columns", {})
                    if not columns:
                        continue

                    col_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
                    pk = table_def.get("primary_key")
                    if pk:
                        col_defs.append(f"PRIMARY KEY ({pk})")

                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                        {', '.join(col_defs)}
                    );
                    """
                    cursor.execute(create_table_sql)
                print("Schema and tables verified successfully.")
            except psycopg2.Error as e:
                print(f"Error during schema creation: {e}")
                self.conn.rollback()
                raise

    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        """
        Perform high-performance native bulk load using COPY.
        This method does NOT commit the transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected to the database. Call connect() first.")
        if data.empty:
            print("DataFrame is empty, skipping bulk load.")
            return

        if mode not in ["append", "overwrite"]:
            raise ValueError("Mode must be either 'append' or 'overwrite'.")

        with self.conn.cursor() as cursor:
            try:
                if mode == "overwrite":
                    truncate_sql = f"TRUNCATE TABLE {schema}.{target_table} RESTART IDENTITY;"
                    print(f"Overwriting table: executing `{truncate_sql}`")
                    cursor.execute(truncate_sql)

                buffer = io.StringIO()
                data.to_csv(buffer, index=False, header=False, sep='\t', na_rep='')
                buffer.seek(0)
                copy_sql = f"COPY {schema}.{target_table} FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '')"
                print(f"Starting bulk load to '{schema}.{target_table}'...")
                cursor.copy_expert(sql=copy_sql, file=buffer)
                print(f"Successfully loaded {len(data)} rows.")
            except (IOError, psycopg2.Error) as e:
                print(f"Error during bulk load: {e}")
                self.conn.rollback()
                raise

    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: list[str], schema: str
    ) -> None:
        """
        Execute a MERGE (Upsert) operation from a staging table.
        This method does NOT commit the transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")
        if not primary_keys:
            raise ValueError("primary_keys must be provided for a merge operation.")

        print(f"Merging data from '{schema}.{staging_table}' to '{schema}.{target_table}'...")

        with self.conn.cursor() as cursor:
            try:
                query = sql.SQL("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s;
                """)
                cursor.execute(query, (schema, staging_table))
                table_cols = [row[0] for row in cursor.fetchall()]

                if not table_cols:
                    print(f"Warning: Staging table '{schema}.{staging_table}' is empty or does not exist. Skipping merge.")
                    return

                update_cols = [col for col in table_cols if col not in primary_keys]
                if not update_cols:
                    print("Warning: No columns to update (all columns are primary keys). Skipping merge.")
                    return

                update_cols_sql = sql.SQL(', ').join(
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in update_cols
                )
                merge_sql = sql.SQL("""
                    INSERT INTO {target} ({cols})
                    SELECT {cols} FROM {staging}
                    ON CONFLICT ({pks}) DO UPDATE
                    SET {update_clause};
                """).format(
                    target=sql.Identifier(schema, target_table),
                    cols=sql.SQL(', ').join(map(sql.Identifier, table_cols)),
                    staging=sql.Identifier(schema, staging_table),
                    pks=sql.SQL(', ').join(map(sql.Identifier, primary_keys)),
                    update_clause=update_cols_sql
                )
                print("Executing MERGE SQL...")
                cursor.execute(merge_sql)
                print(f"Successfully merged. {cursor.rowcount} rows affected.")
            except psycopg2.Error as e:
                print(f"Error during merge operation: {e}")
                self.conn.rollback()
                raise

    def get_latest_state(self, dataset_id: str, schema: str) -> dict:
        """
        Retrieve the latest ingestion state for a dataset from PostgreSQL.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        query = sql.SQL("SELECT last_watermark FROM {schema}.ingestion_state WHERE dataset_id = %s;").format(
            schema=sql.Identifier(schema)
        )
        with self.conn.cursor() as cursor:
            cursor.execute(query, (dataset_id,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            return {}

    def update_state(self, dataset_id: str, state: dict, status: str, schema: str) -> None:
        """
        Update the ingestion state for a dataset.
        This method does NOT commit the transaction.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        pipeline_version = version("py-load-pmda")
        now = datetime.now(timezone.utc)
        last_watermark = json.dumps(state)
        last_successful_ts = now if status == 'SUCCESS' else None

        update_sql = sql.SQL("""
        INSERT INTO {schema}.ingestion_state (
            dataset_id, last_run_ts_utc, last_successful_run_ts_utc,
            status, last_watermark, pipeline_version
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset_id) DO UPDATE SET
            last_run_ts_utc = EXCLUDED.last_run_ts_utc,
            last_successful_run_ts_utc = CASE
                WHEN EXCLUDED.status = 'SUCCESS' THEN EXCLUDED.last_successful_run_ts_utc
                ELSE ingestion_state.last_successful_run_ts_utc
            END,
            status = EXCLUDED.status,
            last_watermark = EXCLUDED.last_watermark,
            pipeline_version = EXCLUDED.pipeline_version;
        """).format(schema=sql.Identifier(schema))

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(update_sql, (
                    dataset_id, now, last_successful_ts, status, last_watermark, pipeline_version
                ))
                print(f"State for dataset '{dataset_id}' updated with status '{status}'.")
            except psycopg2.Error as e:
                print(f"Error updating state for dataset '{dataset_id}': {e}")
                self.conn.rollback()
                raise

    def get_all_states(self, schema: str) -> list[dict]:
        """Retrieve all ingestion states from the database."""
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        query = sql.SQL("SELECT * FROM {schema}.ingestion_state ORDER BY dataset_id;").format(
            schema=sql.Identifier(schema)
        )
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            return [dict(row) for row in results]

    def execute_sql(self, query: str, params: tuple = None) -> None:
        """Executes an arbitrary SQL command."""
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(query, params)
            except psycopg2.Error as e:
                print(f"Error executing custom SQL: {e}")
                self.conn.rollback()
                raise
