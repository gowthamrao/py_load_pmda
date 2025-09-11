from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


class LoaderInterface(ABC):
    """
    Abstract Base Class for database loader implementations.

    This interface ensures that any new database backend added to the system
    adheres to a standard set of operations for connecting, managing schemas,
    loading data, and handling state. It also acts as a context manager
    to ensure connections are properly handled.
    """

    @abstractmethod
    def connect(self, connection_details: Dict[str, Any]) -> None:
        """Establish connection to the target database."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the target database."""
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit the current database transaction."""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Roll back the current database transaction."""
        pass

    @abstractmethod
    def ensure_schema(self, schema_definition: Dict[str, Any]) -> None:
        """
        Ensure the target schema and tables exist.
        This method should be executed within a transaction.
        """
        pass

    @abstractmethod
    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        """
        Perform high-performance native bulk load of the data.
        Mode can be 'append' or 'overwrite'.
        This method should be executed within a transaction.
        """
        pass

    @abstractmethod
    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: List[str], schema: str
    ) -> None:
        """
        Execute a MERGE (Upsert) operation from a staging table to the target table.
        This method should be executed within a transaction.
        """
        pass

    @abstractmethod
    def get_latest_state(self, dataset_id: str, schema: str) -> Dict[str, Any]:
        """Retrieve the latest ingestion state for a dataset."""
        pass

    @abstractmethod
    def update_state(
        self, dataset_id: str, state: Dict[str, Any], status: str, schema: str
    ) -> None:
        """
        Transactionally update the ingestion state after a load.
        This method should be executed within a transaction.
        """
        pass

    @abstractmethod
    def get_all_states(self, schema: str) -> List[Dict[str, Any]]:
        """Retrieve all ingestion states from the database."""
        pass

    @abstractmethod
    def execute_sql(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        """Executes an arbitrary SQL command."""
        pass

    def __enter__(self) -> "LoaderInterface":
        """Enter the context manager, returning the instance."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager, ensuring disconnection."""
        self.disconnect()
