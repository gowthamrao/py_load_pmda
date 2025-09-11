import logging
from typing import Any, Dict, List, Union

import pandas as pd


class DataValidator:
    """
    A class to perform data quality checks on a pandas DataFrame based on a
    set of configurable validation rules.
    """

    def __init__(self, rules: List[Dict[str, Any]]):
        """
        Initializes the DataValidator with a set of validation rules.

        Args:
            rules: A list of dictionaries, where each dictionary defines a
                   validation rule. E.g.,
                   [{'column': 'col_name', 'check': 'not_null'},
                    {'column': 'col_name', 'check': 'is_unique'}]
        """
        self.rules = rules
        self.errors: List[str] = []

    def validate(self, df: pd.DataFrame) -> bool:
        """
        Executes all validation rules against the given DataFrame.

        Args:
            df: The pandas DataFrame to validate.

        Returns:
            True if all checks pass, False otherwise. The details of any
            failures are stored in the `self.errors` list.
        """
        self.errors = []
        if self.rules is None:
            logging.info("No validation rules provided, skipping validation.")
            return True

        for rule in self.rules:
            check_method_name = f"_check_{rule['check']}"
            check_method = getattr(self, check_method_name, None)

            if not check_method:
                self.errors.append(f"Unknown validation check: {rule['check']}")
                continue

            column = rule.get("column")
            if column and column not in df.columns:
                self.errors.append(f"Validation failed: Column '{column}' not found in DataFrame.")
                continue

            try:
                check_method(df, **rule)
            except Exception as e:
                self.errors.append(
                    f"Error during validation check '{rule['check']}' on column '{column}': {e}"
                )

        if self.errors:
            for error in self.errors:
                logging.error(f"Data validation failure: {error}")
            return False

        logging.info("Data validation successful.")
        return True

    def _check_not_null(self, df: pd.DataFrame, column: str, **kwargs: Any) -> None:
        """Checks for null values in a column."""
        if df[column].isnull().any():
            null_count = df[column].isnull().sum()
            self.errors.append(f"Column '{column}' has {null_count} null values.")

    def _check_is_unique(self, df: pd.DataFrame, column: str, **kwargs: Any) -> None:
        """Checks for duplicate values in a column."""
        if not df[column].is_unique:
            duplicates = df[df.duplicated(subset=[column])][column].nunique()
            self.errors.append(
                f"Column '{column}' is not unique. Found {duplicates} duplicate values."
            )

    def _check_has_type(self, df: pd.DataFrame, column: str, type: str, **kwargs: Any) -> None:
        """Checks if a column can be cast to a specific data type."""
        try:
            if type == "integer":
                # Check if all non-null values are integers
                if not pd.to_numeric(df[column], errors="coerce").notna().all():
                    self.errors.append(f"Column '{column}' contains non-integer values.")
            elif type == "float":
                if not pd.to_numeric(df[column], errors="coerce").notna().all():
                    self.errors.append(f"Column '{column}' contains non-float values.")
            elif type == "datetime":
                if pd.to_datetime(df[column], errors="coerce").isnull().any():
                    self.errors.append(f"Column '{column}' contains non-datetime values.")
            else:
                # For other types, we can just check the dtype
                if df[column].dtype.name != type:
                    self.errors.append(
                        f"Column '{column}' has type {df[column].dtype.name}, expected {type}."
                    )
        except Exception as e:
            self.errors.append(f"Type check for column '{column}' failed: {e}")

    def _check_is_in_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Union[int, float],
        max_value: Union[int, float],
        **kwargs: Any,
    ) -> None:
        """Checks if values in a numeric column are within a specified range."""
        if not pd.to_numeric(df[column], errors="coerce").between(min_value, max_value).all():
            out_of_range = df[
                ~pd.to_numeric(df[column], errors="coerce").between(min_value, max_value)
            ]
            self.errors.append(
                f"Column '{column}' has {len(out_of_range)} values outside the range [{min_value}, {max_value}]."
            )

    def _check_is_in_set(
        self, df: pd.DataFrame, column: str, allowed_values: List[Any], **kwargs: Any
    ) -> None:
        """Checks if all values in a column are from a specified set."""
        if not df[column].isin(allowed_values).all():
            invalid_values = df[~df[column].isin(allowed_values)][column].unique()
            self.errors.append(
                f"Column '{column}' contains values not in the allowed set: {list(invalid_values)}"
            )
