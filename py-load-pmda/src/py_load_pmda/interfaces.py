from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd


class LoaderInterface(ABC):
    """
    Abstract Base Class defining the contract for database loader adapters.

    This interface ensures that any new database backend added to the system
    adheres to a standard set of operations for connecting, managing schemas,

    loading data, and handling state.
    """

    @abstractmethod
    def connect(self, connection_details: Dict[str, Any]) -> None:
        """Establish connection to the target database."""
        pass

    @abstractmethod
    def ensure_schema(self, schema_definition: Dict[str, Any]) -> None:
        """Ensure the target schema and tables exist."""
        pass

    @abstractmethod
    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        """
        Perform high-performance native bulk load of the data.
        Mode can be 'append' or 'overwrite'.
        """
        pass

    @abstractmethod
    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: List[str], schema: str
    ) -> None:
        """
        Execute a MERGE (Upsert) operation from a staging table to the target table.
        """
        pass

    @abstractmethod
    def get_latest_state(self, dataset_id: str, schema: str) -> Dict[str, Any]:
        """Retrieve the latest ingestion state for a dataset."""
        pass

    @abstractmethod
    def update_state(self, dataset_id: str, state: Dict[str, Any], status: str, schema: str) -> None:
        """Transactionally update the ingestion state after a load."""
        pass

    @abstractmethod
    def get_all_states(self, schema: str) -> List[Dict[str, Any]]:
        """Retrieve all ingestion states from the database."""
        pass
