from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
import pytest
from typer.testing import CliRunner

from py_load_pmda.cli import app
from py_load_pmda.config import load_config
from py_load_pmda.parser import JaderParser
from py_load_pmda.transformer import JaderTransformer

runner = CliRunner()


@pytest.fixture(scope="module")
def jader_test_zip() -> Path:
    """Fixture to provide the path to the new test JADER zip file."""
    p = Path(__file__).parent / "fixtures" / "test_jader_pipeline.zip"
    assert p.exists(), "Test fixture 'test_jader_pipeline.zip' not found!"
    return p


@pytest.fixture(scope="module")
def jader_parser() -> JaderParser:
    """Fixture to provide a JaderParser instance."""
    return JaderParser()


@pytest.fixture(scope="module")
def jader_transformer() -> JaderTransformer:
    """Fixture to provide a JaderTransformer instance."""
    return JaderTransformer(source_url="http://dummy.url/test_jader_pipeline.zip")


def test_jader_parser(jader_parser: JaderParser, jader_test_zip: Path) -> None:
    """
    Test that the new JaderParser correctly parses the test zip file.
    """
    parsed_data = jader_parser.parse(jader_test_zip)

    # 1. Check that the output is a dictionary with the four expected table names
    assert isinstance(parsed_data, dict)
    assert set(parsed_data.keys()) == {"jader_demo", "jader_drug", "jader_reac", "jader_hist"}

    # 2. Check that each value is a non-empty DataFrame
    for name, df in parsed_data.items():
        assert isinstance(df, pd.DataFrame), f"'{name}' should be a DataFrame"
        assert not df.empty, f"DataFrame '{name}' should not be empty"

    # 3. Check for the Japanese header to ensure Shift-JIS was read correctly
    assert "性別" in parsed_data["jader_demo"].columns
    assert "医薬品名" in parsed_data["jader_drug"].columns
    assert "副作用名" in parsed_data["jader_reac"].columns
    assert "原疾患等" in parsed_data["jader_hist"].columns


def test_jader_transformer(
    jader_transformer: JaderTransformer, jader_parser: JaderParser, jader_test_zip: Path
) -> None:
    """
    Test that the new JaderTransformer correctly transforms the parsed data.
    """
    parsed_data = jader_parser.parse(jader_test_zip)
    transformed_data = jader_transformer.transform(parsed_data)

    # 1. Check that the output is a dictionary with the four expected table names
    assert isinstance(transformed_data, dict)
    assert set(transformed_data.keys()) == {"jader_demo", "jader_drug", "jader_reac", "jader_hist"}

    # 2. Check each transformed DataFrame
    for table_name, df in transformed_data.items():
        assert not df.empty, f"Transformed DataFrame '{table_name}' should not be empty"

        # Check for metadata columns
        assert "_meta_source_url" in df.columns
        assert "_meta_source_content_hash" in df.columns
        assert "raw_data_full" in df.columns
        assert pd.notna(df["raw_data_full"].iloc[0])

        # Check for English column names
        if table_name == "jader_demo":
            assert "gender" in df.columns
            assert "identification_number" in df.columns
        if table_name == "jader_drug":
            assert "drug_name" in df.columns
            assert "drug_id" in df.columns  # Check for generated ID
        if table_name == "jader_reac":
            assert "adverse_event_name" in df.columns
            assert "reac_id" in df.columns  # Check for generated ID
        if table_name == "jader_hist":
            assert "past_medical_history" in df.columns
            assert "hist_id" in df.columns  # Check for generated ID

    # 3. Check relationships
    demo_ids = transformed_data["jader_demo"]["identification_number"]
    assert transformed_data["jader_drug"]["identification_number"].isin(demo_ids).all()
    assert transformed_data["jader_reac"]["identification_number"].isin(demo_ids).all()
    assert transformed_data["jader_hist"]["identification_number"].isin(demo_ids).all()


@pytest.mark.skip(reason="Requires a running database and is out of scope for this task")
@pytest.mark.e2e
def test_jader_cli_pipeline(jader_test_zip: Path, mocker: Any) -> None:
    """
    A full end-to-end test of the JADER pipeline using the CLI.
    This test requires a running PostgreSQL database (handled by testcontainers).
    """
    # Mock the JaderExtractor to return our local test zip file
    # This prevents the test from trying to hit the actual PMDA website.
    mocker.patch(
        "py_load_pmda.extractor.JaderExtractor.extract",
        return_value=(
            jader_test_zip,
            "http://dummy.url/test_jader_pipeline.zip",
            {"etag": "dummy-etag"},
        ),
    )

    # --- Step 1: Initialize the database ---
    result_init = runner.invoke(app, ["init-db"])
    assert result_init.exit_code == 0
    assert "Database initialization complete" in result_init.stdout

    # --- Step 2: Run the JADER pipeline ---
    result_run = runner.invoke(app, ["run", "--dataset", "jader"])
    assert result_run.exit_code == 0
    assert "ETL run for dataset 'jader' completed successfully" in result_run.stdout

    # --- Step 3: Verify the data in the database ---
    db_config = load_config()["database"]
    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config.get("host", "localhost"),
        port=db_config.get("port", 5432),
    )
    cur = conn.cursor()

    try:
        # Check that the four JADER tables were created and populated
        for table in ["jader_demo", "jader_drug", "jader_reac", "jader_hist"]:
            cur.execute(f"SELECT COUNT(*) FROM public.{table};")
            count_result = cur.fetchone()
            assert count_result is not None
            count = count_result[0]
            assert count > 0, f"Table 'public.{table}' was not populated."
            print(f"Verified {count} rows in 'public.{table}'.")

        # Check a specific value to ensure correct data loading
        cur.execute("SELECT gender FROM public.jader_demo WHERE identification_number = '1';")
        gender_result = cur.fetchone()
        assert gender_result is not None
        gender = gender_result[0]
        assert gender == "男性"

        # Check state table
        cur.execute(
            "SELECT status, last_watermark->>'etag' FROM public.ingestion_state WHERE dataset_id = 'jader';"
        )
        state_result = cur.fetchone()
        assert state_result is not None
        state_status, etag = state_result
        assert state_status == "SUCCESS"
        assert etag == "dummy-etag"

    finally:
        cur.close()
        conn.close()
