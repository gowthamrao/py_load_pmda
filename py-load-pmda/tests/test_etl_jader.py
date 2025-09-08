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


def test_jader_transformer_normalized_output(jader_transformer, jader_parser, jader_test_data):
    """
    Test that the JaderTransformer correctly transforms the parsed data
    into a dictionary of normalized DataFrames.
    """
    # First, parse the data
    parsed_data = jader_parser.parse([jader_test_data])

    # Now, transform it
    transformed_data = jader_transformer.transform(parsed_data)

    # 1. Check that the output is a dictionary with the three expected table names
    assert isinstance(transformed_data, dict)
    assert set(transformed_data.keys()) == {"jader_case", "jader_drug", "jader_reaction"}

    # 2. Get the individual dataframes
    case_df = transformed_data["jader_case"]
    drug_df = transformed_data["jader_drug"]
    reac_df = transformed_data["jader_reaction"]

    # 3. Check that each value is a non-empty DataFrame
    assert not case_df.empty
    assert not drug_df.empty
    assert not reac_df.empty

    # 4. Check for expected columns in each DataFrame based on the new schema
    # jader_case
    case_expected_cols = [
        'case_id', 'gender', 'age', 'raw_data_full',
        '_meta_load_ts_utc', '_meta_source_content_hash'
    ]
    for col in case_expected_cols:
        assert col in case_df.columns, f"Expected column '{col}' not in jader_case DataFrame"
    assert case_df.set_index("case_id").index.name == "case_id"

    # jader_drug
    assert 'drug_id' in drug_df.columns
    assert 'case_id' in drug_df.columns
    assert 'drug_generic_name' in drug_df.columns
    assert drug_df.set_index("drug_id").index.name == "drug_id"

    # jader_reaction
    assert 'reaction_id' in reac_df.columns
    assert 'case_id' in reac_df.columns
    assert 'reaction_event_name' in reac_df.columns
    assert reac_df.set_index("reaction_id").index.name == "reaction_id"
    # Check that date conversion was attempted only if the column exists
    if 'reaction_onset_date' in reac_df.columns:
        assert pd.api.types.is_object_dtype(reac_df['reaction_onset_date']) # Should be object of datetime.date

    # 5. Check relationships
    # Every drug and reaction should link to a valid case
    assert drug_df['case_id'].isin(case_df['case_id']).all()
    assert reac_df['case_id'].isin(case_df['case_id']).all()

    # 6. Check raw_data_full in jader_case
    assert pd.notna(case_df['raw_data_full'].iloc[0])
    import json
    try:
        raw_json = json.loads(case_df['raw_data_full'].iloc[0])
        assert "source_case" in raw_json
        assert "source_drugs" in raw_json
        assert "source_reactions" in raw_json
    except (json.JSONDecodeError, AssertionError):
        pytest.fail("The 'raw_data_full' column does not contain valid, structured JSON.")
