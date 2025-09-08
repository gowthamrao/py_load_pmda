import pytest
from pathlib import Path
import pandas as pd
from py_load_pmda.parser import JaderParser
from py_load_pmda.transformer import JaderTransformer

@pytest.fixture(scope="module")
def jader_test_data():
    """Fixture to provide the path to the test JADER zip file."""
    # The dummy file is in tests/fixtures relative to the project root
    return Path(__file__).parent / "fixtures" / "dummy_jader.zip"

@pytest.fixture(scope="module")
def jader_parser():
    """Fixture to provide a JaderParser instance."""
    return JaderParser()

@pytest.fixture(scope="module")
def jader_transformer():
    """Fixture to provide a JaderTransformer instance."""
    return JaderTransformer(source_url="http://dummy.url/jader.zip")


def test_jader_parser(jader_parser, jader_test_data):
    """
    Test that the JaderParser correctly parses the dummy zip file.
    """
    assert jader_test_data.exists(), "Test fixture dummy_jader.zip not found!"

    parsed_data = jader_parser.parse([jader_test_data])

    # 1. Check that the output is a dictionary with the four expected keys
    assert isinstance(parsed_data, dict)
    assert set(parsed_data.keys()) == {"case", "demo", "drug", "reac"}

    # 2. Check that each value is a non-empty DataFrame
    for name, df in parsed_data.items():
        assert isinstance(df, pd.DataFrame), f"'{name}' should be a DataFrame"
        assert not df.empty, f"DataFrame '{name}' should not be empty"

    # 3. Check for a key column in each DataFrame to ensure data was read
    assert "識別番号" in parsed_data["case"].columns
    assert "識別番号" in parsed_data["demo"].columns
    assert "医薬品（一般名）" in parsed_data["drug"].columns
    assert "有害事象" in parsed_data["reac"].columns


def test_jader_transformer(jader_transformer, jader_parser, jader_test_data):
    """
    Test that the JaderTransformer correctly transforms the parsed data.
    """
    # First, parse the data
    parsed_data = jader_parser.parse([jader_test_data])

    # Now, transform it
    transformed_df = jader_transformer.transform(parsed_data)

    # 1. Check that the output is a non-empty DataFrame
    assert isinstance(transformed_df, pd.DataFrame)
    assert not transformed_df.empty

    # 2. Check for a subset of essential columns that should exist based on the dummy data
    expected_cols = [
        'case_id',
        'gender',
        'drug_generic_name',
        'reaction_event_name',
        'raw_data_full',
        '_meta_load_ts_utc',
        '_meta_source_content_hash'
    ]
    for col in expected_cols:
        assert col in transformed_df.columns, f"Expected column '{col}' not in transformed DataFrame"

    # 3. Check that the raw_data_full column contains valid JSON
    assert pd.notna(transformed_df['raw_data_full'].iloc[0])
    import json
    try:
        json.loads(transformed_df['raw_data_full'].iloc[0])
    except json.JSONDecodeError:
        pytest.fail("The 'raw_data_full' column does not contain valid JSON.")
