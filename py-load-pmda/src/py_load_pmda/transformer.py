import pandas as pd
import json
from typing import List
import hashlib
from datetime import datetime, timezone
from importlib.metadata import version
import re
from py_load_pmda import utils

class ApprovalsTransformer:
    """
    Transforms the raw DataFrame of New Drug Approvals into a standardized format.
    """

    def __init__(self, source_url: str):
        self.source_url = source_url

    def _extract_brand_and_applicant(self, series: pd.Series) -> pd.DataFrame:
        """Extracts brand name and applicant from a combined string."""
        # Regex to capture the brand name (non-greedy) and the applicant name inside the last parentheses.
        pattern = re.compile(r'^(.*?)\s*\(([^)]+、[^)]+)\)$', re.DOTALL)

        extracted = series.str.extract(pattern)
        extracted.columns = ['brand_name_jp', 'applicant_name_jp_raw']

        # For rows that didn't match (i.e., no applicant in parentheses),
        # the brand name is the whole string.
        extracted['brand_name_jp'] = extracted['brand_name_jp'].fillna(series)

        # Clean up the applicant name by removing the corporate number
        if 'applicant_name_jp_raw' in extracted.columns:
            extracted['applicant_name_jp'] = extracted['applicant_name_jp_raw'].str.split('、').str[0]
        else:
            extracted['applicant_name_jp'] = None


        # Clean up whitespace and newlines
        extracted['brand_name_jp'] = extracted['brand_name_jp'].str.strip()
        extracted['applicant_name_jp'] = extracted['applicant_name_jp'].str.strip()

        return extracted[['brand_name_jp', 'applicant_name_jp']]

    def transform(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Transforms the raw DataFrames to match the target schema.

        Args:
            dfs: A list of raw DataFrames from the parser.

        Returns:
            A transformed DataFrame ready for loading.
        """
        if not dfs:
            return pd.DataFrame()

        # Concatenate all dataframes into one
        df = pd.concat(dfs, ignore_index=True)

        # 1. Create the raw_data_full column for auditability
        df['raw_data_full'] = df.to_json(orient='records', lines=True).splitlines()

        # 2. Rename columns based on the FRD schema
        rename_map = {
            "分野": "application_type",
            "承認日": "approval_date",
            "No.": "approval_id",
            "販売名(会社名、法人番号)": "brand_applicant_raw",
            "成分名(下線:新有効成分)": "generic_name_jp",
            "効能・効果等": "indication"
        }
        df.rename(columns=rename_map, inplace=True)

        # 3. Extract brand name and applicant
        brand_applicant_df = self._extract_brand_and_applicant(df['brand_applicant_raw'])
        df['brand_name_jp'] = brand_applicant_df['brand_name_jp']
        df['applicant_name_jp'] = brand_applicant_df['applicant_name_jp']

        # 4. Clean and transform data
        # Convert approval_date to ISO 8601 format using the robust utility
        df['approval_date'] = utils.to_iso_date(df['approval_date'])

        # 5. Add metadata columns
        df['_meta_load_ts_utc'] = datetime.now(timezone.utc)
        df['_meta_source_url'] = self.source_url
        df['_meta_pipeline_version'] = version("py-load-pmda")
        df['_meta_source_content_hash'] = df['raw_data_full'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )

        # Add missing columns from FRD schema and set to None
        df['review_report_url'] = None

        # 6. Select and order columns for the final schema
        final_columns = [
            'approval_id',
            'application_type',
            'brand_name_jp',
            'generic_name_jp',
            'applicant_name_jp',
            'approval_date',
            'indication',
            'review_report_url',
            'raw_data_full',
            '_meta_load_ts_utc',
            '_meta_source_content_hash',
            '_meta_source_url',
            '_meta_pipeline_version',
        ]

        # Filter for only the columns that exist in the DataFrame to avoid errors
        existing_final_columns = [col for col in final_columns if col in df.columns]

        return df[existing_final_columns]


class JaderTransformer:
    """
    Transforms the raw JADER DataFrames from the parser into a dictionary of
    normalized, standardized DataFrames ready for loading.
    """

    def __init__(self, source_url: str):
        self.source_url = source_url
        self.pipeline_version = version("py-load-pmda")
        self.extraction_ts = datetime.now(timezone.utc)

        # Mappings from Japanese source columns to English schema columns
        self.COLUMN_MAPS = {
            "jader_demo": {
                '識別番号': 'identification_number', '性別': 'gender', '年齢': 'age',
                '体重': 'weight', '身長': 'height', '報告年度・四半期': 'report_fiscal_year_quarter',
                '転帰': 'outcome', '報告区分': 'report_source', '報告者職種': 'reporter_qualification'
            },
            "jader_drug": {
                '識別番号': 'identification_number', '医薬品の関与': 'drug_involvement',
                '医薬品名': 'drug_name', '使用理由': 'usage_reason'
            },
            "jader_reac": {
                '識別番号': 'identification_number', '副作用名': 'adverse_event_name',
                '発現日': 'onset_date'
            },
            "jader_hist": {
                '識別番号': 'identification_number', '原疾患等': 'past_medical_history'
            }
        }
        self.ID_COLUMNS = {
            "jader_drug": "drug_id",
            "jader_reac": "reac_id",
            "jader_hist": "hist_id"
        }

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds all required metadata columns to a DataFrame."""
        df['_meta_load_ts_utc'] = datetime.now(timezone.utc)
        df['_meta_extraction_ts_utc'] = self.extraction_ts
        df['_meta_source_url'] = self.source_url
        df['_meta_pipeline_version'] = self.pipeline_version
        df['_meta_source_content_hash'] = df['raw_data_full'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )
        return df

    def _generate_hash_id(self, df: pd.DataFrame, id_col_name: str) -> pd.DataFrame:
        """Generates a unique ID for each row by hashing its contents."""
        # Use a subset of columns that define uniqueness, excluding the raw data itself
        # to ensure the hash is stable even if raw_data_full format changes.
        cols_to_hash = [col for col in df.columns if col not in ['raw_data_full'] and not col.startswith('_meta')]

        def default_converter(o):
            """Handle non-serializable types for JSON dumping."""
            if pd.isna(o):
                return None
            return str(o) # Fallback to string representation

        df[id_col_name] = df.apply(
            lambda row: hashlib.sha256(
                json.dumps(
                    row[cols_to_hash].to_dict(),
                    sort_keys=True,
                    default=default_converter
                ).encode('utf-8')
            ).hexdigest(),
            axis=1
        )
        return df

    def transform(self, data_frames: dict) -> dict[str, pd.DataFrame]:
        """
        Transforms the four raw JADER DataFrames.

        Args:
            data_frames: A dictionary of raw DataFrames from the JaderParser.

        Returns:
            A dictionary of transformed DataFrames ready for loading.
        """
        transformed_dfs = {}

        for table_name, df_raw in data_frames.items():
            if df_raw is None or df_raw.empty:
                print(f"No data for '{table_name}', skipping transformation.")
                transformed_dfs[table_name] = pd.DataFrame()
                continue

            print(f"Transforming data for '{table_name}'...")
            df = df_raw.copy()

            # 1. Create the raw_data_full column for high-fidelity audit trails
            df['raw_data_full'] = df.to_json(orient='records', lines=True, force_ascii=False).splitlines()

            # 2. Rename columns from Japanese to standard English names
            rename_map = self.COLUMN_MAPS.get(table_name, {})
            df.rename(columns=rename_map, inplace=True)

            # Drop original columns that were not in the rename map, except the primary key
            schema_cols = list(rename_map.values())
            if 'identification_number' not in schema_cols:
                 schema_cols.append('identification_number')

            df = df[schema_cols + ['raw_data_full']]


            # 3. Handle special data transformations
            if table_name == 'jader_reac' and 'onset_date' in df.columns:
                df['onset_date'] = utils.to_iso_date(df['onset_date'])

            # 4. Generate a unique hash ID for tables that need it
            if table_name in self.ID_COLUMNS:
                id_col = self.ID_COLUMNS[table_name]
                df = self._generate_hash_id(df, id_col)

            # 5. Add standard metadata columns
            df = self._add_metadata(df)

            # 6. Ensure final DataFrame has all columns from the schema, even if empty
            # This would be where you might cross-reference the schema definition
            # from schemas.py, but for now, we rely on the rename map.

            transformed_dfs[table_name] = df

        return transformed_dfs


class PackageInsertsTransformer:
    """
    Transforms a raw DataFrame from a Package Insert PDF into a standardized format.
    """
    def __init__(self, source_url: str):
        self.source_url = source_url

    def transform(self, dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """
        Transforms the raw DataFrames to match a generic document schema.

        Since the content of the PDF tables is not yet known, this transformer
        focuses on meeting the data fidelity requirements of the FRD by storing
        the entire raw content in a JSONB-compatible column and adding metadata.

        Args:
            dfs: A list of raw DataFrames from the PackageInsertsParser.

        Returns:
            A transformed, single-row DataFrame ready for loading.
        """
        if not dfs:
            return pd.DataFrame()

        # Concatenate all tables into one DataFrame for easier serialization
        full_df = pd.concat(dfs, ignore_index=True)
        # Convert all data to string to prevent JSON serialization errors with mixed types
        full_df = full_df.astype(str)


        # 1. Create the 'raw_data_full' JSONB object
        # This captures all the extracted tables in a structured way.
        raw_data_full = {
            "source_file_type": "pdf",
            "extracted_tables": full_df.to_dict(orient='records')
        }
        raw_data_full_json = json.dumps(raw_data_full, ensure_ascii=False)

        # 2. Create a primary key for the document.
        # We use a hash of the source URL for a deterministic ID.
        document_id = hashlib.sha256(self.source_url.encode('utf-8')).hexdigest()

        # 3. Create the single-row DataFrame
        transformed_data = {
            "document_id": document_id,
            "raw_data_full": raw_data_full_json,
            "_meta_source_url": self.source_url,
            "_meta_extraction_ts_utc": datetime.now(timezone.utc),
            "_meta_load_ts_utc": datetime.now(timezone.utc), # Placeholder, will be updated at load time
            "_meta_pipeline_version": version("py-load-pmda"),
            "_meta_source_content_hash": hashlib.sha256(raw_data_full_json.encode('utf-8')).hexdigest()
        }

        final_df = pd.DataFrame([transformed_data])

        # 4. Define and order final columns
        final_columns = [
            'document_id',
            'raw_data_full',
            '_meta_source_url',
            '_meta_extraction_ts_utc',
            '_meta_load_ts_utc',
            '_meta_pipeline_version',
            '_meta_source_content_hash'
        ]
        return final_df[final_columns]


class ReviewReportsTransformer:
    """
    Transforms a list of raw DataFrames from a Review Report PDF into a standardized format.
    """
    def __init__(self, source_url: str):
        self.source_url = source_url

    def transform(self, dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """
        Transforms the raw DataFrames to match a generic document schema.

        This transformer captures all extracted tables into a single JSONB-compatible
        column for data fidelity, as required by the FRD.

        Args:
            dfs: A list of raw DataFrames from the ReviewReportsParser.

        Returns:
            A transformed, single-row DataFrame ready for loading.
        """
        if not dfs:
            return pd.DataFrame()

        # Concatenate all tables into one DataFrame for easier serialization
        full_df = pd.concat(dfs, ignore_index=True)
        # Convert all data to string to prevent JSON serialization errors with mixed types
        full_df = full_df.astype(str)

        # 1. Create the 'raw_data_full' JSONB object
        raw_data_full = {
            "source_file_type": "pdf",
            "extracted_tables": full_df.to_dict(orient='records')
        }
        raw_data_full_json = json.dumps(raw_data_full, ensure_ascii=False)

        # 2. Create a primary key for the document using a hash of the source URL
        document_id = hashlib.sha256(self.source_url.encode('utf-8')).hexdigest()

        # 3. Create the single-row DataFrame for loading
        transformed_data = {
            "document_id": document_id,
            "raw_data_full": raw_data_full_json,
            "_meta_source_url": self.source_url,
            "_meta_extraction_ts_utc": datetime.now(timezone.utc),
            "_meta_load_ts_utc": datetime.now(timezone.utc),
            "_meta_pipeline_version": version("py-load-pmda"),
            "_meta_source_content_hash": hashlib.sha256(raw_data_full_json.encode('utf-8')).hexdigest()
        }

        final_df = pd.DataFrame([transformed_data])

        # 4. Define and order final columns
        final_columns = [
            'document_id',
            'raw_data_full',
            '_meta_source_url',
            '_meta_extraction_ts_utc',
            '_meta_load_ts_utc',
            '_meta_pipeline_version',
            '_meta_source_content_hash'
        ]
        return final_df[final_columns]
