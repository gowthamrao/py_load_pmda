import pytest
import pandas as pd
from datetime import date
from py_load_pmda import utils

@pytest.mark.parametrize(
    "input_date, expected_date",
    [
        ("2025-09-08", date(2025, 9, 8)),
        ("2023.01.15", date(2023, 1, 15)),
        ("令和7年9月8日", date(2025, 9, 8)),  # Reiwa 7 = 2019 + 7 - 1 = 2025
        ("平成31年4月30日", date(2019, 4, 30)), # Heisei 31 = 1989 + 31 - 1 = 2019
        ("昭和64年1月7日", date(1989, 1, 7)),  # Showa 64 = 1926 + 64 - 1 = 1989
        ("令和元年5月1日", date(2019, 5, 1)),   # Gannen (first year)
        ("Invalid Date", pd.NaT),
        (None, pd.NaT),
        ("", pd.NaT),
    ],
)
def test_to_iso_date(input_date, expected_date):
    """
    Tests the to_iso_date utility with various date formats.
    """
    input_series = pd.Series([input_date])
    result_series = utils.to_iso_date(input_series)

    result_val = result_series.iloc[0]

    if pd.isna(expected_date):
        assert pd.isna(result_val)
    else:
        assert result_val == expected_date
