import io
import json
from datetime import datetime, timezone
from importlib.metadata import version
import pandas as pd
import psycopg2
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

    def ensure_schema(self, schema_definition: dict) -> None:
        """
        Ensure the target schema and tables exist in PostgreSQL.

        Args:
            schema_definition: A dictionary defining the schema and tables.
                               Example:
                               {
                                   "schema_name": "my_schema",
                                   "tables": {
                                       "my_table": {
                                           "columns": {"id": "INTEGER", "data": "TEXT"},
                                           "primary_key": "id"
                                       }
                                   }
                               }
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

                    # Generate column definitions
                    col_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]

                    # Add primary key constraint if specified
                    pk = table_def.get("primary_key")
                    if pk:
                        col_defs.append(f"PRIMARY KEY ({pk})")

                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                        {', '.join(col_defs)}
                    );
                    """
                    cursor.execute(create_table_sql)

                self.conn.commit()
                print("Schema and tables verified successfully.")
            except psycopg2.Error as e:
                print(f"Error during schema creation: {e}")
                self.conn.rollback()
                raise

    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        """
        Perform high-performance native bulk load of the data using COPY.

        Args:
            data: The pandas DataFrame to load.
            target_table: The name of the target table.
            schema: The name of the target schema.
            mode: Load mode. 'append' or 'overwrite'.
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

                # Use an in-memory buffer to stream data
                buffer = io.StringIO()
                # Use tab as a separator, which is less likely to be in the data.
                # Ensure no quotes are used and nulls are represented as empty strings.
                data.to_csv(buffer, index=False, header=False, sep='\t', na_rep='')
                buffer.seek(0)

                copy_sql = f"COPY {schema}.{target_table} FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '')"

                print(f"Starting bulk load to '{schema}.{target_table}'...")
                cursor.copy_expert(sql=copy_sql, file=buffer)

                self.conn.commit()
                print(f"Successfully loaded {len(data)} rows.")

            except (IOError, psycopg2.Error) as e:
                print(f"Error during bulk load: {e}")
                self.conn.rollback()
                raise

    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: list[str], schema: str
    ) -> None:
        """
        Execute a MERGE (Upsert) operation from a staging table to a target
        table using PostgreSQL's 'INSERT ... ON CONFLICT' statement.

        Args:
            staging_table: The name of the table with the new data.
            target_table: The name of the table to merge into.
            primary_keys: A list of column names that form the primary key
                          or a unique constraint for conflict detection.
            schema: The database schema for the tables.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")
        if not primary_keys:
            raise ValueError("primary_keys must be provided for a merge operation.")

        print(f"Merging data from '{schema}.{staging_table}' to '{schema}.{target_table}'...")

        with self.conn.cursor() as cursor:
            try:
                # Safely query for column names using parameter substitution for values
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

                # Safely construct the query using psycopg2.sql objects
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

                self.conn.commit()

            except psycopg2.Error as e:
                print(f"Error during merge operation: {e}")
                self.conn.rollback()
                raise

    def get_latest_state(self, dataset_id: str, schema: str = "public") -> dict:
        """
        Retrieve the latest ingestion state for a dataset from PostgreSQL.
        The state is assumed to be in a table named 'ingestion_state'.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        query = f"SELECT last_watermark FROM {schema}.ingestion_state WHERE dataset_id = %s;"

        with self.conn.cursor() as cursor:
            cursor.execute(query, (dataset_id,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            return {}

    def update_state(self, dataset_id: str, state: dict, status: str, schema: str = "public") -> None:
        """
        Transactionally update the ingestion state after a load in PostgreSQL.
        """
        if not self.conn:
            raise ConnectionError("Not connected. Call connect() first.")

        pipeline_version = version("py-load-pmda")
        now = datetime.now(timezone.utc)
        last_watermark = json.dumps(state)

        update_sql = f"""
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
        """

        last_successful_ts = now if status == 'SUCCESS' else None

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(update_sql, (
                    dataset_id, now, last_successful_ts, status, last_watermark, pipeline_version
                ))
                self.conn.commit()
                print(f"State for dataset '{dataset_id}' updated with status '{status}'.")
            except psycopg2.Error as e:
                print(f"Error updating state for dataset '{dataset_id}': {e}")
                self.conn.rollback()
                raise
