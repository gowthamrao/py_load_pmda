import hashlib
import json
import logging
from datetime import datetime, timezone

import pandas as pd

from py_load_pmda.schemas import TABLES_SCHEMA


def convert_wareki_to_ad(wareki_date: str) -> pd.Timestamp | None:
    """
    Convert a Japanese Wareki date string to a Western AD timestamp.

    Handles formats like "令和7年9月8日".

    Args:
        wareki_date: The date string in Wareki format.

    Returns:
        A pandas Timestamp object, or None if parsing fails.
    """
    if not isinstance(wareki_date, str) or not wareki_date:
        return None

    try:
        era_name = wareki_date[:2]
        date_parts = wareki_date[2:].replace("年", "-").replace("月", "-").replace("日", "")
        year_str, month_str, day_str = date_parts.split("-")
        year = int(year_str)

        era_starts = {
            "令和": 2019,
            "平成": 1989,
            "昭和": 1926,
            "大正": 1912,
            "明治": 1868,
        }

        if era_name not in era_starts:
            logging.warning(f"Unknown era name: {era_name} in date '{wareki_date}'")
            return None

        # The Western year is (Era Start Year - 1) + Japanese Year
        ad_year = era_starts[era_name] + year - 1
        return pd.to_datetime(f"{ad_year}-{month_str}-{day_str}")
    except (ValueError, IndexError) as e:
        logging.warning(f"Could not parse Wareki date '{wareki_date}': {e}")
        return None


class BaseTransformer:
    """
    Base class for all transformers.
    """

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform a raw DataFrame into the standard schema.
        This method should be implemented by subclasses.
        """
        raise NotImplementedError


class ApprovalsTransformer(BaseTransformer):
    """
    Transformer for New Drug Approval data.
    """

    # Mapping from Japanese Excel column names to the standard schema column names
    COLUMN_MAP = {
        "承認番号": "approval_id",
        "申請区分": "application_type",
        "販売名": "brand_name_jp",
        "一般名": "generic_name_jp",
        "申請者": "applicant_name_jp",
        "承認日": "approval_date",
        "効能・効果": "indication",
        "審査報告書": "review_report_url",  # Assuming the URL is in this column
    }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean, standardize, and restructure the raw approvals DataFrame.

        Args:
            df: The raw DataFrame from the parser.

        Returns:
            A DataFrame conforming to the `pmda_approvals` schema.
        """
        if df.empty:
            return pd.DataFrame()

        logging.info("Starting transformation of approvals data...")

        # 1. Rename columns to match the standard schema
        df = df.rename(columns=self.COLUMN_MAP)

        # 2. Generate the 'raw_data_full' JSONB column before any transformations
        def create_raw_data_full(row):
            original_dict = row.to_dict()
            # We don't need the source file in the JSON blob itself
            original_dict.pop("_meta_source_file", None)
            return json.dumps(
                {
                    "source_file_name": row.get("_meta_source_file", ""),
                    "original_values": original_dict,
                },
                ensure_ascii=False,
                default=str,
            )

        df["raw_data_full"] = df.apply(create_raw_data_full, axis=1)

        # 3. Convert 'approval_date' from Wareki to AD
        if "approval_date" in df.columns:
            df["approval_date"] = df["approval_date"].apply(convert_wareki_to_ad)

        # 4. Add metadata columns
        df["_meta_load_ts_utc"] = datetime.now(timezone.utc)
        df["_meta_source_content_hash"] = df["raw_data_full"].apply(
            lambda x: hashlib.sha256(x.encode()).hexdigest()
        )

        # 5. Select and reorder columns to match the final schema
        final_columns = list(TABLES_SCHEMA["pmda_approvals"]["columns"].keys())
        # Ensure all required columns exist, filling missing ones with None (or pd.NA)
        for col in final_columns:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[final_columns]

        logging.info(f"Successfully transformed {len(df)} rows.")
        return df
