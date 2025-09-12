import pandas as pd
import pytest
from typer.testing import CliRunner

from py_load_pmda.cli import app
from py_load_pmda.config import load_config

runner = CliRunner()


# Fixture to mock the database adapter
@pytest.fixture
def mock_db_adapter(mocker):
    mock_adapter_instance = mocker.MagicMock()
    mock_adapter_instance.get_latest_state.return_value = {}
    mock_adapter_instance.get_all_states.return_value = []
    # Mock the factory where it's used by the 'run' command's orchestrator
    mocker.patch("py_load_pmda.orchestrator.get_db_adapter", return_value=mock_adapter_instance)
    return mock_adapter_instance


# Fixture to mock the ETL components
@pytest.fixture
def mock_etl_components(mocker):
    # Mock extractor to return a dummy file path and state
    mock_extractor_instance = mocker.MagicMock()
    mock_extractor_instance.extract.return_value = (
        "/path/to/dummy.zip",
        "http://dummy.url/jader.zip",
        {"etag": "new-etag"},
    )
    mocker.patch(
        "py_load_pmda.orchestrator.AVAILABLE_EXTRACTORS",
        {"JaderExtractor": lambda **kwargs: mock_extractor_instance},
    )

    # Mock parser to return some initial data
    mock_parser_instance = mocker.MagicMock()
    mocker.patch(
        "py_load_pmda.orchestrator.AVAILABLE_PARSERS", {"JaderParser": lambda: mock_parser_instance}
    )

    # Mock transformer
    mock_transformer_instance = mocker.MagicMock()
    mocker.patch(
        "py_load_pmda.orchestrator.AVAILABLE_TRANSFORMERS",
        {"JaderTransformer": lambda **kwargs: mock_transformer_instance},
    )

    return mock_extractor_instance, mock_parser_instance, mock_transformer_instance


# Initial data for the first run
initial_data = {
    "jader_demo": pd.DataFrame({"identification_number": [1], "gender": ["Male"]}),
    "jader_drug": pd.DataFrame(
        {"drug_id": ["drug1"], "identification_number": [1], "drug_name": ["Aspirin"]}
    ),
    "jader_reac": pd.DataFrame(
        {"reac_id": ["reac1"], "identification_number": [1], "adverse_event_name": ["Headache"]}
    ),
    "jader_hist": pd.DataFrame(
        {"hist_id": ["hist1"], "identification_number": [1], "past_medical_history": ["None"]}
    ),
}

# Data for the second run (with one new record and one updated record per table)
updated_data = {
    "jader_demo": pd.DataFrame(
        {
            "identification_number": [1, 2],
            "gender": ["Male", "Female"],  # Update 1, New 2
        }
    ),
    "jader_drug": pd.DataFrame(
        {
            "drug_id": ["drug1", "drug2"],
            "identification_number": [1, 2],
            "drug_name": ["Aspirin Forte", "Ibuprofen"],  # Update drug1, New drug2
        }
    ),
    "jader_reac": pd.DataFrame(
        {
            "reac_id": ["reac1", "reac2"],
            "identification_number": [1, 2],
            "adverse_event_name": ["Severe Headache", "Nausea"],  # Update reac1, New reac2
        }
    ),
    "jader_hist": pd.DataFrame(
        {
            "hist_id": ["hist1", "hist2"],
            "identification_number": [1, 2],
            "past_medical_history": ["Hypertension", "Asthma"],  # Update hist1, New hist2
        }
    ),
}


def test_jader_pipeline_merge_logic(mock_db_adapter, mock_etl_components, mocker, monkeypatch):
    """
    Tests that the JADER ETL pipeline correctly uses the merge (upsert) strategy
    for its multiple tables when `load_mode: merge` is set in the config.
    """
    monkeypatch.setenv("PMDA_DB_PASSWORD", "testpassword")
    _, mock_parser, mock_transformer = mock_etl_components

    # --- First Run (Initial Load) ---
    print("--- Running Initial Load ---")
    mock_transformer.transform.return_value = initial_data
    # The parser result doesn't matter much as the transformer is mocked
    mock_parser.parse.return_value = "dummy_parser_output"

    result1 = runner.invoke(app, ["run", "--dataset", "jader"])
    assert result1.exit_code == 0, result1.stdout

    # Verify merge was called for each of the 4 tables
    assert mock_db_adapter.execute_merge.call_count == 4

    # Check that the merge calls were for the correct tables
    expected_tables = ["jader_demo", "jader_drug", "jader_reac", "jader_hist"]
    called_tables = [
        call.kwargs["target_table"] for call in mock_db_adapter.execute_merge.call_args_list
    ]
    assert sorted(called_tables) == sorted(expected_tables)

    # Check the primary keys used for merge
    jader_config = load_config()["datasets"]["jader"]
    for call in mock_db_adapter.execute_merge.call_args_list:
        table_name = call.kwargs["target_table"]
        expected_pk = jader_config["tables"][table_name]["primary_key"]
        assert call.kwargs["primary_keys"] == expected_pk

    # --- Second Run (Update and Insert) ---
    print("--- Running Update/Insert Load ---")
    # Reset mocks for the second run
    mock_db_adapter.reset_mock()
    mock_transformer.reset_mock()

    # Simulate that the first run's state is now the "last_state"
    mock_db_adapter.get_latest_state.return_value = {"etag": "old-etag"}
    mock_transformer.transform.return_value = updated_data

    result2 = runner.invoke(app, ["run", "--dataset", "jader"])
    assert result2.exit_code == 0, result2.stdout

    # Verify merge was called again for each of the 4 tables
    assert mock_db_adapter.execute_merge.call_count == 4
    called_tables_run2 = [
        call.kwargs["target_table"] for call in mock_db_adapter.execute_merge.call_args_list
    ]
    assert sorted(called_tables_run2) == sorted(expected_tables)

    # Verify that the data loaded into the staging table (via bulk_load) was the updated data
    assert mock_db_adapter.bulk_load.call_count == 4

    # Check the data for one of the tables to be sure
    for call in mock_db_adapter.bulk_load.call_args_list:
        if call.kwargs["target_table"] == "staging_jader_drug":
            loaded_df = call.kwargs["data"]
            pd.testing.assert_frame_equal(loaded_df, updated_data["jader_drug"])
            break
    else:
        pytest.fail("Did not find bulk_load call for staging_jader_drug")

    # Verify state was updated at the end
    # The schema for the state table is also dynamically generated by fixtures,
    # so we get the actual schema name used from the get_latest_state call.
    state_schema_used = mock_db_adapter.get_latest_state.call_args.kwargs["schema"]
    mock_db_adapter.update_state.assert_called_with(
        "jader",
        state={"etag": "new-etag"},
        status="SUCCESS",
        schema=state_schema_used,
    )
