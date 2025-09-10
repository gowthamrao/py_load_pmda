import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from importlib.metadata import version
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

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
        # This now accepts either a Japanese or Western comma.
        pattern = re.compile(r'^(.*?)\s*\(([^)]+[、,][^)]+)\)$', re.DOTALL)

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
        Transforms the raw DataFrames to match the target schema. This involves
        aggregating rows that share the same approval ID.
        """
        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)

        # 1. Preserve original data for the raw_data_full column before any changes
        # The `to_json` call will be done during aggregation.
        df['original_row'] = df.to_dict(orient='records')

        # 2. Rename columns
        rename_map = {
            "分野": "application_type",
            "承認日": "approval_date_str", # Keep as string for now
            "No.": "approval_id",
            "販売名(会社名、法人番号)": "brand_applicant_raw",
            "成分名(下線:新有効成分)": "generic_name_jp",
            "効能・効果等": "indication"
        }
        df.rename(columns=rename_map, inplace=True)

        # 3. Pre-process fields that need extraction before aggregation
        brand_applicant_df = self._extract_brand_and_applicant(df['brand_applicant_raw'])
        df['brand_name_jp'] = brand_applicant_df['brand_name_jp']
        df['applicant_name_jp'] = brand_applicant_df['applicant_name_jp']
        df['approval_date'] = utils.to_iso_date(df['approval_date_str'])

        # 4. Define aggregation logic
        # For text fields, join unique, non-null values. For others, take the first.
        agg_funcs = {
            'application_type': 'first',
            'approval_date': 'first',
            'brand_name_jp': lambda x: '\n'.join(x.dropna().unique()),
            'generic_name_jp': lambda x: '\n'.join(x.dropna().unique()),
            'applicant_name_jp': lambda x: '\n'.join(x.dropna().unique()),
            'indication': lambda x: '\n'.join(x.dropna().unique()),
            # Use pandas' to_json, which correctly handles NaN -> null conversion.
            'original_row': lambda x: pd.Series(list(x)).to_json(orient='values', force_ascii=False)
        }

        # Select only columns that exist in the dataframe to avoid errors during aggregation
        cols_to_agg = {k: v for k, v in agg_funcs.items() if k in df.columns}

        # 5. Group by approval_id and aggregate
        df_agg = df.groupby('approval_id').agg(cols_to_agg).reset_index()

        # Rename the aggregated 'original_row' to 'raw_data_full'
        df_agg.rename(columns={'original_row': 'raw_data_full'}, inplace=True)

        # Cast approval_id to integer for correct data type
        df_agg['approval_id'] = df_agg['approval_id'].astype(int)

        # 6. Add metadata columns
        df_agg['_meta_load_ts_utc'] = datetime.now(timezone.utc)
        df_agg['_meta_source_url'] = self.source_url
        df_agg['_meta_pipeline_version'] = version("py-load-pmda")
        df_agg['_meta_source_content_hash'] = df_agg['raw_data_full'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )
        df_agg['review_report_url'] = None # Add missing column

        # 7. Select and order columns for the final schema
        final_columns = [
            'approval_id', 'application_type', 'brand_name_jp', 'generic_name_jp',
            'applicant_name_jp', 'approval_date', 'indication', 'review_report_url',
            'raw_data_full', '_meta_load_ts_utc', '_meta_source_content_hash',
            '_meta_source_url', '_meta_pipeline_version',
        ]
        return df_agg[final_columns]


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

        def default_converter(o: Any) -> Any:
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

    def transform(self, data_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
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
                logging.info(f"No data for '{table_name}', skipping transformation.")
                transformed_dfs[table_name] = pd.DataFrame()
                continue

            logging.info(f"Transforming data for '{table_name}'...")
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
    Transforms raw data from a Package Insert PDF into a standardized format.
    """
    def __init__(self, source_url: str):
        self.source_url = source_url

    def transform(self, parsed_data: Tuple[str, List[pd.DataFrame]]) -> pd.DataFrame:
        """
        Transforms the parsed PDF data to match a generic document schema.
        This version handles the new parser output but does not extract structured data.
        """
        full_text, tables = parsed_data
        if not full_text and not tables:
            return pd.DataFrame()

        # Convert tables to dicts for JSON serialization
        tables_as_dicts = [df.to_dict(orient='records') for df in tables]

        raw_data_full = {
            "source_file_type": "pdf",
            "full_text": full_text,
            "extracted_tables": tables_as_dicts
        }
        raw_data_full_json = json.dumps(raw_data_full, ensure_ascii=False)
        document_id = hashlib.sha256(self.source_url.encode('utf-8')).hexdigest()

        transformed_data = {
            "document_id": document_id,
            "raw_data_full": raw_data_full_json,
            "_meta_source_url": self.source_url,
            "_meta_extraction_ts_utc": datetime.now(timezone.utc),
            "_meta_load_ts_utc": datetime.now(timezone.utc),
            "_meta_pipeline_version": version("py-load-pmda"),
            "_meta_source_content_hash": hashlib.sha256(raw_data_full_json.encode('utf-8')).hexdigest()
        }
        return pd.DataFrame([transformed_data])


class ReviewReportsTransformer:
    """
    Transforms parsed data from a Review Report PDF into a structured format.
    """
    def __init__(self, source_url: str) -> None:
        self.source_url = source_url

    def _find_value_after_keyword(self, text: str, keyword: str) -> Optional[str]:
        """Finds the first non-empty string on the same line after a keyword."""
        try:
            pattern = re.compile(f"^{keyword}: (.*)$", re.MULTILINE)
            match = pattern.search(text)
            if match:
                return match.group(1).strip()
        except Exception:
            pass
        return None

    def _find_summary(self, text: str) -> Optional[str]:
        """Extracts the summary section of the report."""
        try:
            # Simpler regex: find the keyword and capture everything after it until the next major section
            # This is less brittle than assuming what the next section starts with.
            pattern = re.compile(r"審査の概要\s*\n(.*?)(?=\n\s*\d+\.|\Z)", re.DOTALL)
            match = pattern.search(text)
            if match:
                return match.group(1).strip()
        except Exception:
            pass
        return None


    def transform(self, parsed_data: Tuple[str, List[pd.DataFrame]]) -> pd.DataFrame:
        """
        Transforms the raw text and tables into a structured DataFrame.
        """
        full_text, tables = parsed_data
        if not full_text:
            return pd.DataFrame()

        # 1. Extract structured data using regex and keyword searches
        brand_name = self._find_value_after_keyword(full_text, "販売名")
        generic_name = self._find_value_after_keyword(full_text, "一般的名称")
        applicant = self._find_value_after_keyword(full_text, "申請者名")
        app_date_str = self._find_value_after_keyword(full_text, "申請年月日")
        app_date = utils.to_iso_date(pd.Series([app_date_str]))[0]
        approval_date_str = self._find_value_after_keyword(full_text, "承認年月日")
        approval_date = utils.to_iso_date(pd.Series([approval_date_str]))[0]
        summary = self._find_summary(full_text)


        # 2. Create the high-fidelity raw_data_full column
        tables_as_dicts = [df.to_dict(orient='records') for df in tables]
        raw_data_full = {
            "source_file_type": "pdf",
            "full_text": full_text,
            "extracted_tables": tables_as_dicts
        }
        raw_data_full_json = json.dumps(raw_data_full, ensure_ascii=False)

        # 3. Create the document ID and metadata
        document_id = hashlib.sha256(self.source_url.encode('utf-8')).hexdigest()
        content_hash = hashlib.sha256(raw_data_full_json.encode('utf-8')).hexdigest()
        now = datetime.now(timezone.utc)
        pipeline_version = version("py-load-pmda")

        # 4. Assemble the final DataFrame
        transformed_data = {
            "document_id": document_id,
            "brand_name_jp": brand_name,
            "generic_name_jp": generic_name,
            "applicant_name_jp": applicant,
            "application_date": app_date,
            "approval_date": approval_date,
            "review_summary_text": summary,
            "raw_data_full": raw_data_full_json,
            "_meta_source_url": self.source_url,
            "_meta_extraction_ts_utc": now,
            "_meta_load_ts_utc": now, # Placeholder
            "_meta_pipeline_version": pipeline_version,
            "_meta_source_content_hash": content_hash
        }

        final_df = pd.DataFrame([transformed_data])
        return final_df
