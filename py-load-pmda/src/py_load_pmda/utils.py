import re
from datetime import date
from typing import Any

import chardet
import pandas as pd

# Define Japanese era start years (Gregorian)
WAREKI_ERA_STARTS = {
    "令和": 2019, "平成": 1989, "昭和": 1926, "大正": 1912, "明治": 1868,
}

# Regex to capture Wareki date parts.
WAREKI_PATTERN = re.compile(
    r"(?P<era>{eras})(?P<year>\d+|元)年"
    r"(?P<month>\d{{1,2}})月(?P<day>\d{{1,2}})日".format(
        eras="|".join(WAREKI_ERA_STARTS.keys())
    )
)

def to_iso_date(series: pd.Series) -> pd.Series:
    """
    Converts a pandas Series of dates in various formats (including Japanese
    Wareki) to ISO 8601 date objects.
    """
    def convert_single_date(d: Any) -> Any:
        if pd.isna(d) or not isinstance(d, str):
            return pd.NaT

        # 1. Try standard parsing first. `errors='coerce'` handles failures gracefully.
        # Also handles formats like 'YYYY.MM.DD'
        dt = pd.to_datetime(d.replace('年', '-').replace('月', '-').replace('日', ''), errors='coerce')
        if pd.notna(dt):
            return dt

        # 2. If standard parsing fails, try Wareki parsing
        clean_d = d.strip().replace(" ", "").replace("　", "")
        match = WAREKI_PATTERN.match(clean_d)
        if not match:
            return pd.NaT

        parts = match.groupdict()
        era = parts["era"]
        year_str = parts["year"]
        month = int(parts["month"])
        day = int(parts["day"])

        year = 1 if year_str == "元" else int(year_str)
        era_start_year = WAREKI_ERA_STARTS.get(era)
        if not era_start_year:
            return pd.NaT

        gregorian_year = era_start_year + year - 1
        try:
            return pd.to_datetime(date(gregorian_year, month, day))
        except ValueError:
            return pd.NaT

    return series.apply(convert_single_date).dt.date  # type: ignore


def detect_encoding(data: bytes, fallback: str = 'utf-8') -> str:
    """Detects the character encoding of a byte string."""
    if not isinstance(data, bytes) or not data:
        return fallback

    result = chardet.detect(data)
    encoding = result.get('encoding')
    confidence = result.get('confidence', 0)

    if confidence < 0.7 or not encoding:
        print(f"Encoding detection uncertain (confidence: {confidence:.2f}). Falling back to '{fallback}'.")
        return fallback

    print(f"Detected encoding: '{encoding}' with {confidence:.2f} confidence.")
    return encoding
