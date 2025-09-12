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
    },
}

# Schema for the New Drug Approvals data
PMDA_APPROVALS_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "pmda_approvals": {
            "columns": {
                "approval_id": "INTEGER",
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
    },
}

# Schema for the JADER (Japanese Adverse Drug Event Report) data
# This schema is normalized into four tables to align with the FRD and the
# four source CSV files (DEMO, DRUG, REAC, HIST).
PMDA_JADER_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "jader_demo": {
            "columns": {
                "identification_number": "VARCHAR(50) NOT NULL",
                "gender": "VARCHAR(10)",
                "age": "VARCHAR(20)",
                "weight": "VARCHAR(20)",
                "height": "VARCHAR(20)",
                "report_fiscal_year_quarter": "VARCHAR(50)",
                "outcome": "TEXT",
                "report_source": "TEXT",
                "reporter_qualification": "TEXT",
                "raw_data_full": "JSONB",
                "_meta_load_ts_utc": "TIMESTAMPTZ",
                "_meta_extraction_ts_utc": "TIMESTAMPTZ",
                "_meta_source_url": "TEXT",
                "_meta_pipeline_version": "VARCHAR(50)",
                "_meta_source_content_hash": "VARCHAR(64)",
            },
            "primary_key": "identification_number",
        },
        "jader_drug": {
            "columns": {
                "drug_id": "VARCHAR(64) NOT NULL",  # A unique hash of the row
                "identification_number": "VARCHAR(50) NOT NULL",  # FK to jader_demo
                "drug_involvement": "TEXT",
                "drug_name": "TEXT",
                "usage_reason": "TEXT",
                "raw_data_full": "JSONB",
                "_meta_load_ts_utc": "TIMESTAMPTZ",
                "_meta_extraction_ts_utc": "TIMESTAMPTZ",
                "_meta_source_url": "TEXT",
                "_meta_pipeline_version": "VARCHAR(50)",
                "_meta_source_content_hash": "VARCHAR(64)",
            },
            "primary_key": "drug_id",
        },
        "jader_reac": {
            "columns": {
                "reac_id": "VARCHAR(64) NOT NULL",  # A unique hash of the row
                "identification_number": "VARCHAR(50) NOT NULL",  # FK to jader_demo
                "adverse_event_name": "TEXT",
                "onset_date": "DATE",
                "raw_data_full": "JSONB",
                "_meta_load_ts_utc": "TIMESTAMPTZ",
                "_meta_extraction_ts_utc": "TIMESTAMPTZ",
                "_meta_source_url": "TEXT",
                "_meta_pipeline_version": "VARCHAR(50)",
                "_meta_source_content_hash": "VARCHAR(64)",
            },
            "primary_key": "reac_id",
        },
        "jader_hist": {
            "columns": {
                "hist_id": "VARCHAR(64) NOT NULL",  # A unique hash of the row
                "identification_number": "VARCHAR(50) NOT NULL",  # FK to jader_demo
                "past_medical_history": "TEXT",
                "raw_data_full": "JSONB",
                "_meta_load_ts_utc": "TIMESTAMPTZ",
                "_meta_extraction_ts_utc": "TIMESTAMPTZ",
                "_meta_source_url": "TEXT",
                "_meta_pipeline_version": "VARCHAR(50)",
                "_meta_source_content_hash": "VARCHAR(64)",
            },
            "primary_key": "hist_id",
        },
    },
}

# Schema for the Package Inserts data
PMDA_PACKAGE_INSERTS_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "pmda_package_inserts": {
            "columns": {
                "document_id": "VARCHAR(64) NOT NULL",
                "raw_data_full": "JSONB",
                "_meta_source_url": "TEXT",
                "_meta_extraction_ts_utc": "TIMESTAMPTZ",
                "_meta_load_ts_utc": "TIMESTAMPTZ",
                "_meta_pipeline_version": "VARCHAR(50)",
                "_meta_source_content_hash": "VARCHAR(64)",
            },
            "primary_key": "document_id",
        }
    },
}


# Schema for the Review Reports data
PMDA_REVIEW_REPORTS_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "pmda_review_reports": {
            "columns": {
                "document_id": "VARCHAR(64) NOT NULL",
                "brand_name_jp": "TEXT",
                "generic_name_jp": "TEXT",
                "applicant_name_jp": "TEXT",
                "application_date": "DATE",
                "approval_date": "DATE",
                "review_summary_text": "TEXT",
                "raw_data_full": "JSONB",
                "_meta_source_url": "TEXT",
                "_meta_extraction_ts_utc": "TIMESTAMPTZ",
                "_meta_load_ts_utc": "TIMESTAMPTZ",
                "_meta_pipeline_version": "VARCHAR(50)",
                "_meta_source_content_hash": "VARCHAR(64)",
            },
            "primary_key": "document_id",
        }
    },
}


# Schema for the integration test of the DataValidator
VALIDATION_TEST_SCHEMA = {
    "schema_name": "public",
    "tables": {
        "validation_test_table": {
            "columns": {
                "id": "INTEGER",
                "category": "TEXT",
                "value": "INTEGER",
            },
            "primary_key": "id",
        }
    },
}


# A dictionary to map dataset IDs to their schema definitions
DATASET_SCHEMAS = {
    "approvals": PMDA_APPROVALS_SCHEMA,
    "jader": PMDA_JADER_SCHEMA,
    "package_inserts": PMDA_PACKAGE_INSERTS_SCHEMA,
    "review_reports": PMDA_REVIEW_REPORTS_SCHEMA,
    # Add the test schema so the orchestrator can find it during tests
    "validation_test_dataset": VALIDATION_TEST_SCHEMA,
}
