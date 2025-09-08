import pandas as pd
import json
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

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw DataFrame to match the target schema.

        Args:
            df: The raw DataFrame from the parser.

        Returns:
            A transformed DataFrame ready for loading.
        """
        if df.empty:
            return pd.DataFrame()

        # Create a copy to avoid SettingWithCopyWarning
        df = df.copy()

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
    Transforms the raw JADER DataFrames into a single, standardized DataFrame.
    """
    def __init__(self, source_url: str):
        self.source_url = source_url
        # Define column name mappings
        self.rename_map = {
            '識別番号': 'case_id',
            '報告回数': 'report_count',
            '性別': 'gender',
            '年齢': 'age',
            '体重': 'weight',
            '身長': 'height',
            '報告年度・四半期': 'report_fiscal_quarter',
            '状況': 'status',
            '報告の種類': 'report_type',
            '報告者の資格': 'reporter_qualification',
            '医薬品の関与': 'drug_involvement',
            '医薬品（一般名）': 'drug_generic_name',
            '医薬品（販売名）': 'drug_brand_name',
            '使用理由': 'drug_usage_reason',
            '有害事象': 'reaction_event_name',
            '転帰': 'reaction_outcome',
            '有害事象の発現日': 'reaction_onset_date',
        }

    def _aggregate_to_json(self, df: pd.DataFrame, group_by_col: str) -> pd.DataFrame:
        """Groups a DataFrame and aggregates the rows into a JSON string."""
        return (
            df.groupby(group_by_col)
            .apply(lambda x: x.to_json(orient='records', force_ascii=False))
            .reset_index(name='aggregated_json')
        )

    def transform(self, data_frames: dict) -> pd.DataFrame:
        """
        Transforms the raw JADER data into a single, analysis-ready DataFrame.
        """
        case_df = data_frames.get("case")
        demo_df = data_frames.get("demo")
        drug_df = data_frames.get("drug")
        reac_df = data_frames.get("reac")

        if case_df is None or demo_df is None or drug_df is None or reac_df is None:
            raise ValueError("One or more required JADER dataframes are missing.")

        # --- 1. Create the `raw_data_full` representation ---
        # For each case, we want a JSON object containing the original rows from all tables.
        # We'll aggregate drug and reaction data first.
        drug_agg = self._aggregate_to_json(drug_df, '識別番号')
        reac_agg = self._aggregate_to_json(reac_df, '識別番号')

        # Merge these aggregated JSON strings with the demo table
        raw_merged = demo_df.merge(drug_agg, on='識別番号', how='left').merge(reac_agg, on='識別番号', how='left')
        raw_merged.rename(columns={'aggregated_json_x': 'drugs_raw', 'aggregated_json_y': 'reactions_raw'}, inplace=True)

        # Now create the final raw JSON object for each case
        raw_merged['raw_data_full'] = raw_merged.to_json(orient='records', lines=True).splitlines()

        # --- 2. Create the Standard Representation ---
        # Merge the main dataframes. This can create many rows per case.
        # The DEMO, DRUG, and REAC files do not contain '報告回数', so we merge on '識別番号' only.
        merged_df = case_df.merge(demo_df, on='識別番号', how='left')
        merged_df = merged_df.merge(drug_df, on='識別番号', how='left')
        merged_df = merged_df.merge(reac_df, on='識別番号', how='left')

        # --- 3. Clean and Standardize ---
        merged_df.rename(columns=self.rename_map, inplace=True)

        # Use the robust date conversion utility
        if 'reaction_onset_date' in merged_df.columns:
            merged_df['reaction_onset_date'] = utils.to_iso_date(merged_df['reaction_onset_date'])

        # Add the raw_data_full column by merging it back
        final_df = merged_df.merge(raw_merged[['識別番号', 'raw_data_full']], left_on='case_id', right_on='識別番号', how='left')

        # --- 4. Add Metadata ---
        final_df['_meta_load_ts_utc'] = datetime.now(timezone.utc)
        final_df['_meta_source_url'] = self.source_url
        final_df['_meta_pipeline_version'] = version("py-load-pmda")
        final_df['_meta_source_content_hash'] = final_df['raw_data_full'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest() if pd.notna(x) else None
        )

        # --- 5. Final Column Selection ---
        # This part will need a proper schema definition later
        final_columns = list(self.rename_map.values()) + [
            'raw_data_full', '_meta_load_ts_utc', '_meta_source_url',
            '_meta_pipeline_version', '_meta_source_content_hash'
        ]
        existing_cols = [col for col in final_columns if col in final_df.columns]

        return final_df[existing_cols]
