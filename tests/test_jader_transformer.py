import pytest
import pandas as pd
import json
from py_load_pmda.transformer import JaderTransformer

@pytest.fixture
def sample_jader_dataframes():
    """Provides a sample dictionary of JADER DataFrames."""
    case_data = {"識別番号": [1], "報告回数": [1]}
    demo_data = {"識別番号": [1], "性別": [1], "年齢": ["30-39歳"]}
    drug_data = {
        "識別番号": [1, 1],
        "報告回数": [1, 1],
        "医薬品（一般名）": ["アスピリン", "テスト薬"],
        "医薬品の関与": ["被疑薬", "被疑薬"],
    }
    reac_data = {
        "識別番号": [1, 1],
        "報告回数": [1, 1],
        "有害事象": ["頭痛", "吐き気"],
    }
    return {
        "case": pd.DataFrame(case_data),
        "demo": pd.DataFrame(demo_data),
        "drug": pd.DataFrame(drug_data),
        "reac": pd.DataFrame(reac_data),
    }

def test_jader_transformer_basic_merge(sample_jader_dataframes):
    """
    Tests the basic merging and transformation logic of JaderTransformer.
    """
    transformer = JaderTransformer(source_url="dummy_url")
    transformed_df = transformer.transform(sample_jader_dataframes)

    # A case with 2 drugs and 2 reactions should result in 4 rows
    assert len(transformed_df) == 4

    # Check for renamed columns
    assert "case_id" in transformed_df.columns
    assert "gender" in transformed_df.columns
    assert "drug_generic_name" in transformed_df.columns
    assert "reaction_event_name" in transformed_df.columns

    # Check that metadata columns are present
    assert "_meta_load_ts_utc" in transformed_df.columns
    assert "_meta_source_url" in transformed_df.columns

    # Check that case_id is consistent
    assert transformed_df["case_id"].nunique() == 1
    assert transformed_df["case_id"].iloc[0] == 1

def test_jader_transformer_raw_data_full(sample_jader_dataframes):
    """
    Tests the creation of the 'raw_data_full' JSON column.
    """
    transformer = JaderTransformer(source_url="dummy_url")
    transformed_df = transformer.transform(sample_jader_dataframes)

    assert "raw_data_full" in transformed_df.columns

    # All rows for the same case should have the same raw_data_full
    assert transformed_df["raw_data_full"].nunique() == 1

    # Inspect the content of the JSON
    raw_json_content = json.loads(transformed_df["raw_data_full"].iloc[0])

    assert raw_json_content["識別番号"] == 1
    assert "drugs_raw" in raw_json_content
    assert "reactions_raw" in raw_json_content

    # The raw drugs and reactions should be JSON strings themselves
    drugs_raw = json.loads(raw_json_content["drugs_raw"])
    assert len(drugs_raw) == 2
    assert drugs_raw[0]["医薬品（一般名）"] == "アスピリン"

    reactions_raw = json.loads(raw_json_content["reactions_raw"])
    assert len(reactions_raw) == 2
    assert reactions_raw[0]["有害事象"] == "頭痛"
