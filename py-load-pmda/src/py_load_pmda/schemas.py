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

# Schema for the JADER (Japanese Adverse Drug Event Report) data
# This schema is normalized into three tables to align with the FRD.
PMDA_JADER_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "jader_case": {
            "columns": {
                "case_id": "VARCHAR(50) NOT NULL",
                "report_count": "INTEGER",
                "gender": "VARCHAR(10)",
                "age": "VARCHAR(20)",
                "weight": "VARCHAR(20)",
                "height": "VARCHAR(20)",
                "report_fiscal_quarter": "VARCHAR(20)",
                "status": "TEXT",
                "report_type": "TEXT",
                "reporter_qualification": "TEXT",
                "raw_data_full": "JSONB",
                "_meta_load_ts_utc": "TIMESTAMPTZ",
                "_meta_source_url": "TEXT",
                "_meta_pipeline_version": "VARCHAR(50)",
                "_meta_source_content_hash": "VARCHAR(64)",
            },
            "primary_key": "case_id",
        },
        "jader_drug": {
            "columns": {
                "drug_id": "VARCHAR(64) NOT NULL",  # A unique hash of the row
                "case_id": "VARCHAR(50) NOT NULL",  # FK to jader_case
                "drug_involvement": "TEXT",
                "drug_generic_name": "TEXT",
                "drug_brand_name": "TEXT",
                "drug_usage_reason": "TEXT",
            },
            "primary_key": "drug_id",
            # Foreign key constraints are not explicitly defined here to maintain
            # adapter-agnosticism, but are expected to be enforced by the loader if possible.
        },
        "jader_reaction": {
            "columns": {
                "reaction_id": "VARCHAR(64) NOT NULL",  # A unique hash of the row
                "case_id": "VARCHAR(50) NOT NULL",  # FK to jader_case
                "reaction_event_name": "TEXT",
                "reaction_outcome": "TEXT",
                "reaction_onset_date": "DATE",
            },
            "primary_key": "reaction_id",
        },
    },
}

# A dictionary to map dataset IDs to their schema definitions
DATASET_SCHEMAS = {
    "approvals": PMDA_APPROVALS_SCHEMA,
    "jader": PMDA_JADER_SCHEMA,
}
