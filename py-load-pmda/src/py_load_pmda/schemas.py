"""
Centralized database schema definitions for the py-load-pmda package.
"""

# Schema for the metadata/state management table
INGESTION_STATE_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "ingestion_state": {
            "columns": {
                "dataset_id": "VARCHAR(100) NOT NULL",
                "last_run_ts_utc": "TIMESTAMPTZ",
                "last_successful_run_ts_utc": "TIMESTAMPTZ",
                "status": "VARCHAR(50)",
                "last_watermark": "JSONB",
                "pipeline_version": "VARCHAR(50)",
            },
            "primary_key": "dataset_id",
        }
    }
}

# Schema for the New Drug Approvals data
PMDA_APPROVALS_SCHEMA = {
    "schema_name": "public",
    "tables": {
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
                "_meta_source_url": "TEXT",
                "_meta_pipeline_version": "VARCHAR(50)",
            },
            # Using a composite key can be tricky if approval_id is not unique
            # across different dates, which it should be. Let's assume
            # approval_id is the true primary key for now.
            "primary_key": "approval_id",
        }
    }
}

# Placeholder schema for the JADER dataset
PMDA_JADER_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "pmda_jader": {
            "columns": {
                "case_id": "VARCHAR(100) NOT NULL",
                # ... more columns to be defined ...
            },
            "primary_key": "case_id",
        }
    }
}

# A dictionary to map dataset IDs to their schema definitions
DATASET_SCHEMAS = {
    "approvals": PMDA_APPROVALS_SCHEMA,
    "jader": PMDA_JADER_SCHEMA,
}
