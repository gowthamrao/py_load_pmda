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
    Transforms the raw JADER DataFrames into a dictionary of normalized,
    standardized DataFrames ready for loading into separate tables.
    """
    def __init__(self, source_url: str):
        self.source_url = source_url
        self.pipeline_version = version("py-load-pmda")

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames columns from Japanese to standardized English names."""
        rename_map = {
            '識別番号': 'case_id', '報告回数': 'report_count', '性別': 'gender',
            '年齢': 'age', '体重': 'weight', '身長': 'height',
            '報告年度・四半期': 'report_fiscal_quarter', '状況': 'status',
            '報告の種類': 'report_type', '報告者の資格': 'reporter_qualification',
            '医薬品の関与': 'drug_involvement', '医薬品（一般名）': 'drug_generic_name',
            '医薬品（販売名）': 'drug_brand_name', '使用理由': 'drug_usage_reason',
            '有害事象': 'reaction_event_name', '転帰': 'reaction_outcome',
            '有害事象の発現日': 'reaction_onset_date',
        }
        df.rename(columns=rename_map, inplace=True)
        return df

    def _generate_hash_id(self, df: pd.DataFrame, id_col_name: str) -> pd.DataFrame:
        """Generates a unique ID for each row by hashing its contents."""
        # Ensure consistent dict ordering for hashing
        df[id_col_name] = df.apply(
            lambda row: hashlib.sha256(
                json.dumps(row.to_dict(), sort_keys=True).encode('utf-8')
            ).hexdigest(),
            axis=1
        )
        return df

    def transform(self, data_frames: dict) -> dict[str, pd.DataFrame]:
        """
        Transforms the raw JADER data into a dictionary of analysis-ready DataFrames.
        Returns:
            A dictionary of DataFrames for 'jader_case', 'jader_drug', 'jader_reaction'.
        """
        case_df = data_frames.get("case")
        demo_df = data_frames.get("demo")
        drug_df = data_frames.get("drug")
        reac_df = data_frames.get("reac")

        if any(df is None for df in [case_df, demo_df, drug_df, reac_df]):
            raise ValueError("One or more required JADER dataframes are missing.")

        # --- 1. Rename all columns first for consistency ---
        case_df_orig = case_df.copy()
        demo_df_orig = demo_df.copy()
        drug_df_orig = drug_df.copy()
        reac_df_orig = reac_df.copy()

        case_df = self._rename_columns(case_df.copy())
        demo_df = self._rename_columns(demo_df.copy())
        drug_df = self._rename_columns(drug_df.copy())
        reac_df = self._rename_columns(reac_df.copy())

        # --- 2. Transform `jader_drug` table ---
        drug_cols_to_select = [
            'case_id', 'drug_involvement', 'drug_generic_name',
            'drug_brand_name', 'drug_usage_reason'
        ]
        existing_drug_cols = [col for col in drug_cols_to_select if col in drug_df.columns]
        drug_df_transformed = drug_df[existing_drug_cols]
        drug_df_transformed = self._generate_hash_id(drug_df_transformed, 'drug_id')

        # --- 3. Transform `jader_reaction` table ---
        reac_cols_to_select = [
            'case_id', 'reaction_event_name', 'reaction_outcome', 'reaction_onset_date'
        ]
        existing_reac_cols = [col for col in reac_cols_to_select if col in reac_df.columns]
        reac_df_transformed = reac_df[existing_reac_cols]

        if 'reaction_onset_date' in reac_df_transformed.columns:
            reac_df_transformed['reaction_onset_date'] = utils.to_iso_date(reac_df_transformed['reaction_onset_date'])
        reac_df_transformed = self._generate_hash_id(reac_df_transformed, 'reaction_id')

        # --- 4. Transform `jader_case` table ---
        case_df_transformed = pd.merge(case_df, demo_df, on='case_id', how='left')

        # Create the `raw_data_full` JSONB column for auditability
        raw_data = {}
        for case_id in case_df['case_id'].unique():
            raw_data[case_id] = {
                "source_case": case_df_orig[case_df_orig['識別番号'] == case_id].to_dict(orient='records'),
                "source_demo": demo_df_orig[demo_df_orig['識別番号'] == case_id].to_dict(orient='records'),
                "source_drugs": drug_df_orig[drug_df_orig['識別番号'] == case_id].to_dict(orient='records'),
                "source_reactions": reac_df_orig[reac_df_orig['識別番号'] == case_id].to_dict(orient='records'),
            }

        case_df_transformed['raw_data_full'] = case_df_transformed['case_id'].map(
            lambda cid: json.dumps(raw_data.get(cid), ensure_ascii=False) if raw_data.get(cid) else None
        )

        # Add metadata
        load_ts = datetime.now(timezone.utc)
        case_df_transformed['_meta_load_ts_utc'] = load_ts
        case_df_transformed['_meta_source_url'] = self.source_url
        case_df_transformed['_meta_pipeline_version'] = self.pipeline_version
        case_df_transformed['_meta_source_content_hash'] = case_df_transformed['raw_data_full'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest() if pd.notna(x) else None
        )

        # Select final columns
        case_cols = [
            'case_id', 'report_count', 'gender', 'age', 'weight', 'height',
            'report_fiscal_quarter', 'status', 'report_type', 'reporter_qualification',
            'raw_data_full', '_meta_load_ts_utc', '_meta_source_url',
            '_meta_pipeline_version', '_meta_source_content_hash'
        ]
        # Ensure all columns are present, adding missing ones as None
        for col in case_cols:
            if col not in case_df_transformed:
                case_df_transformed[col] = None

        case_df_transformed = case_df_transformed[case_cols]

        return {
            "jader_case": case_df_transformed,
            "jader_drug": drug_df_transformed,
            "jader_reaction": reac_df_transformed
        }


class PackageInsertsTransformer:
    """
    Transforms a raw DataFrame from a Package Insert PDF into a standardized format.
    """
    def __init__(self, source_url: str):
        self.source_url = source_url

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw DataFrame to match a generic document schema.

        Since the content of the PDF tables is not yet known, this transformer
        focuses on meeting the data fidelity requirements of the FRD by storing
        the entire raw content in a JSONB-compatible column and adding metadata.

        Args:
            df: The raw DataFrame from the PackageInsertsParser.

        Returns:
            A transformed, single-row DataFrame ready for loading.
        """
        if df.empty:
            return pd.DataFrame()

        # 1. Create the 'raw_data_full' JSONB object
        # This captures all the extracted tables in a structured way.
        raw_data_full = {
            "source_file_type": "pdf",
            "extracted_tables": df.to_dict(orient='records')
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
