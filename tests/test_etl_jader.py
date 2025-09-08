import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import MagicMock

from py_load_pmda.extractor import JaderExtractor
from py_load_pmda.parser import JaderParser
from py_load_pmda.transformer import JaderTransformer

@pytest.fixture
def jader_zip_fixture():
    """Provides the path to the dummy JADER zip file."""
    path = Path("tests/fixtures/dummy_jader.zip")
    if not path.exists():
        pytest.fail("dummy_jader.zip not found.")
    return path

def test_jader_full_etl_flow(mocker, jader_zip_fixture):
    """
    Tests the full JADER ETL flow from extraction to a mock loader.
    """
    # 1. Mock the Extractor to return the local fixture path
    mock_extractor_instance = MagicMock(spec=JaderExtractor)
    mock_extractor_instance.extract.return_value = ([jader_zip_fixture], "dummy_url")
    mocker.patch("py_load_pmda.cli.AVAILABLE_EXTRACTORS", {
        "JaderExtractor": lambda: mock_extractor_instance
    })

    # The real parser and transformer will be used.

    # 2. Mock the loader to capture the data sent to it
    mock_adapter_instance = MagicMock()
    mocker.patch("py_load_pmda.cli.get_db_adapter", return_value=mock_adapter_instance)

    # 3. Run the pipeline via the CLI runner
    from py_load_pmda.cli import app
    from typer.testing import CliRunner
    runner = CliRunner()

    # We need a config that points to our mock Jader classes
    mock_config = {
        "database": {"type": "postgres"},
        "datasets": {
            "jader": {
                "extractor": "JaderExtractor",
                "parser": "JaderParser",
                "transformer": "JaderTransformer",
                "table_name": "pmda_jader",
                "schema_name": "public"
            }
        }
    }
    mocker.patch("py_load_pmda.cli.load_config", return_value=mock_config)

    # Invoke the 'run' command for the 'jader' dataset
    result = runner.invoke(app, ["run", "--dataset", "jader"])

    # 4. Assertions
    assert result.exit_code == 0
    assert "ETL run for dataset 'jader' completed successfully" in result.stdout

    # Check that the loader's bulk_load was called once
    mock_adapter_instance.bulk_load.assert_called_once()

    # Get the DataFrame that was passed to the loader
    call_args = mock_adapter_instance.bulk_load.call_args
    loaded_df = call_args.kwargs['data']

    # Verify the content of the final loaded DataFrame
    assert isinstance(loaded_df, pd.DataFrame)
    assert not loaded_df.empty
    assert "case_id" in loaded_df.columns
    assert "drug_generic_name" in loaded_df.columns
    assert loaded_df["drug_generic_name"].iloc[0] == "アスピリン"
    assert "_meta_source_url" in loaded_df.columns
    assert loaded_df["_meta_source_url"].iloc[0] == "dummy_url"
