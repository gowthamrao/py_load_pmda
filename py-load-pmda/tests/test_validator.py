import pandas as pd
import pytest

from py_load_pmda.validator import DataValidator


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Provides a sample DataFrame for testing."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["A", "B", "C", "D", None],
        "age": [25, 30, 35, 40, 45],
        "category": ["X", "Y", "X", "Y", "Z"],
        "value": [10.1, 20.2, 30.3, 40.4, 50.5],
    }
    return pd.DataFrame(data)


def test_validate_no_rules(sample_df):
    """Test that validation passes when no rules are provided."""
    validator = DataValidator(rules=[])
    assert validator.validate(sample_df) is True
    assert not validator.errors


def test_check_not_null_failure(sample_df):
    """Test the 'not_null' check for a column with nulls."""
    rules = [{"column": "name", "check": "not_null"}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is False
    assert len(validator.errors) == 1
    assert "has 1 null values" in validator.errors[0]


def test_check_not_null_success(sample_df):
    """Test the 'not_null' check for a column without nulls."""
    rules = [{"column": "id", "check": "not_null"}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is True


def test_check_is_unique_failure(sample_df):
    """Test the 'is_unique' check for a column with duplicates."""
    df = sample_df.copy()
    df.loc[5] = [6, "C", 50, "Z", 60.6]  # Duplicate name 'C'
    rules = [{"column": "name", "check": "is_unique"}]
    validator = DataValidator(rules)
    assert validator.validate(df) is False
    assert "is not unique" in validator.errors[0]


def test_check_is_unique_success(sample_df):
    """Test the 'is_unique' check for a unique column."""
    rules = [{"column": "id", "check": "is_unique"}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is True


def test_check_has_type_failure():
    """Test the 'has_type' check with a type mismatch."""
    df = pd.DataFrame({"a": [1, "two", 3]})
    rules = [{"column": "a", "check": "has_type", "type": "integer"}]
    validator = DataValidator(rules)
    assert validator.validate(df) is False
    assert "contains non-integer values" in validator.errors[0]


def test_check_has_type_success(sample_df):
    """Test the 'has_type' check with a correct type."""
    rules = [{"column": "age", "check": "has_type", "type": "integer"}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is True


def test_check_is_in_range_failure(sample_df):
    """Test the 'is_in_range' check with values outside the range."""
    rules = [{"column": "age", "check": "is_in_range", "min_value": 30, "max_value": 40}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is False
    assert "values outside the range" in validator.errors[0]


def test_check_is_in_range_success(sample_df):
    """Test the 'is_in_range' check with all values in range."""
    rules = [{"column": "age", "check": "is_in_range", "min_value": 20, "max_value": 50}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is True


def test_check_is_in_set_failure(sample_df):
    """Test the 'is_in_set' check with invalid values."""
    rules = [{"column": "category", "check": "is_in_set", "allowed_values": ["X", "Y"]}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is False
    assert "not in the allowed set" in validator.errors[0]


def test_check_is_in_set_success(sample_df):
    """Test the 'is_in_set' check with all valid values."""
    rules = [{"column": "category", "check": "is_in_set", "allowed_values": ["X", "Y", "Z"]}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is True

def test_unknown_check(sample_df):
    """Test that an unknown check type is handled gracefully."""
    rules = [{"column": "id", "check": "is_magic"}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is False
    assert "Unknown validation check: is_magic" in validator.errors[0]


def test_missing_column(sample_df):
    """Test that a rule for a missing column is handled gracefully."""
    rules = [{"column": "non_existent_col", "check": "not_null"}]
    validator = DataValidator(rules)
    assert validator.validate(sample_df) is False
    assert "Column 'non_existent_col' not found" in validator.errors[0]
