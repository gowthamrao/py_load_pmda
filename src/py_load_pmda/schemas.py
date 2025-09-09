"""
This module defines the database schemas for the PMDA ETL pipeline.

The schemas are represented as Python dictionaries, which can be used by
database adapters to create the necessary tables and columns.
"""

from typing import Any, Dict

# Schema for the main data tables
# The keys are table names and the values are their definitions.
TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    "pmda_approvals": {
        "columns": {
            "approval_id": "VARCHAR(100)",
            "application_type": "VARCHAR(50)",
            "brand_name_jp": "TEXT",
            "generic_name_jp": "TEXT",
            "applicant_name_jp": "TEXT",
            "approval_date": "DATE",
            "indication": "TEXT",
            "review_report_url": "TEXT",
            "raw_data_full": "JSONB",
            "_meta_load_ts_utc": "TIMESTAMPTZ",
            "_meta_source_content_hash": "VARCHAR(64)",
        },
        "primary_key": "approval_id",
    },
    # Add other data tables here as they are implemented
    # e.g., "jader", "review_reports", "package_inserts"
}

# Schema for the metadata and state management table
METADATA_SCHEMA: Dict[str, Dict[str, Any]] = {
    "ingestion_state": {
        "columns": {
            "dataset_id": "VARCHAR(100)",
            "last_run_ts_utc": "TIMESTAMPTZ",
            "last_successful_run_ts_utc": "TIMESTAMPTZ",
            "status": "VARCHAR(50)",
            "last_watermark": "JSONB",
            "pipeline_version": "VARCHAR(50)",
        },
        "primary_key": "dataset_id",
    }
}

# The complete database schema, including all tables
DB_SCHEMA: Dict[str, Dict[str, Any]] = {**TABLES_SCHEMA, **METADATA_SCHEMA}
