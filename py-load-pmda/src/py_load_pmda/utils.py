import pandas as pd
import re
from datetime import date

# Define Japanese era start years (Gregorian)
# Era starts on Year N, so Era Year 1 corresponds to Gregorian Year N.
# Gregorian Year = (Era Start Year) + (Era Year) - 1
WAREKI_ERA_STARTS = {
    "令和": 2019,  # Reiwa
    "平成": 1989,  # Heisei
    "昭和": 1926,  # Showa
    "大正": 1912,  # Taisho
    "明治": 1868,  # Meiji
}

# Regex to capture Wareki date parts. Example: 令和7年9月8日
# It handles 元年 (first year) as well.
WAREKI_PATTERN = re.compile(
    r"(?P<era>{eras})(?P<year>\d+|元)年"
    r"(?P<month>\d{{1,2}})月(?P<day>\d{{1,2}})日".format(
        eras="|".join(WAREKI_ERA_STARTS.keys())
    )
)

def to_iso_date(series: pd.Series) -> pd.Series:
    """
    Converts a pandas Series containing dates in various formats (including
    Japanese Wareki) to ISO 8601 date format (YYYY-MM-DD).

    Args:
        series: A pandas Series of strings.

    Returns:
        A pandas Series of datetime objects.
    """

    def convert_single_date(d):
        if pd.isna(d) or not isinstance(d, str):
            return pd.NaT

        # Try Wareki parsing first, as it's more specific.
        match = WAREKI_PATTERN.match(d.strip())
        if not match:
            # If Wareki fails, try standard parsing
            try:
                return pd.to_datetime(d, errors="coerce")
            except (ValueError, TypeError):
                return pd.NaT

        parts = match.groupdict()
        era = parts["era"]
        year_str = parts["year"]
        month = int(parts["month"])
        day = int(parts["day"])

        # Handle '元年' (gannen), the first year of an era
        year = 1 if year_str == "元" else int(year_str)

        era_start_year = WAREKI_ERA_STARTS.get(era)
        if not era_start_year:
            return pd.NaT

        gregorian_year = era_start_year + year - 1

        try:
            return pd.to_datetime(date(gregorian_year, month, day))
        except ValueError:
            return pd.NaT

    # Apply the conversion function. Using `errors='coerce'` will turn
    # any values that could not be converted into NaT (Not a Time).
    converted_series = series.apply(convert_single_date)
    return pd.to_datetime(converted_series, errors='coerce').dt.date
