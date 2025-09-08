import pandas as pd
import json
import hashlib
from datetime import datetime, timezone
from importlib.metadata import version
import re

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
        # Convert approval_date to ISO 8601 format
        df['approval_date'] = pd.to_datetime(df['approval_date'], format='%Y.%m.%d').dt.date

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
