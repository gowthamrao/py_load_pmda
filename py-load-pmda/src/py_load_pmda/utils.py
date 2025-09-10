import logging
from typing import Any

import chardet
from jpdatetime import jpdatetime
import pandas as pd


def to_iso_date(series: pd.Series) -> pd.Series:
    """
    Converts a pandas Series of dates in various formats (including Japanese
    Wareki) to ISO 8601 date objects using the jpdatetime library for robust
    parsing.
    """

    def convert_single_date(d: Any) -> Any:
        if pd.isna(d) or not isinstance(d, str):
            return pd.NaT

        # 1. Try standard `pd.to_datetime` first. It handles many common formats.
        # The `errors='coerce'` flag will return NaT for parsing failures.
        dt = pd.to_datetime(
            d.replace("年", "-").replace("月", "-").replace("日", ""), errors="coerce"
        )
        if pd.notna(dt):
            return dt.date()

        # 2. If standard parsing fails, try Wareki parsing with jpdatetime.
        # This library is specifically designed for Japanese era dates.
        try:
            # Clean the string for parsing
            clean_d = d.strip().replace(" ", "").replace("　", "")
            # The format "%G年%m月%d日" interprets the era name and year together (%G).
            # The library correctly handles "元年" for the first year of an era,
            # as well as Arabic numerals for the year.
            parsed_dt = jpdatetime.strptime(clean_d, "%G年%m月%d日")
            return parsed_dt.date()
        except (ValueError, TypeError):
            # If jpdatetime also fails, return NaT (Not a Time)
            logging.debug(f"Could not parse date: {d}")
            return pd.NaT

    return series.apply(convert_single_date)


def detect_encoding(data: bytes, fallback: str = 'utf-8') -> str:
    """Detects the character encoding of a byte string."""
    if not isinstance(data, bytes) or not data:
        return fallback

    result = chardet.detect(data)
    encoding = result.get('encoding')
    confidence = result.get('confidence', 0)

    if confidence < 0.7 or not encoding:
        logging.warning(f"Encoding detection uncertain (confidence: {confidence:.2f}). Falling back to '{fallback}'.")
        return fallback

    logging.debug(f"Detected encoding: '{encoding}' with {confidence:.2f} confidence.")
    return encoding
