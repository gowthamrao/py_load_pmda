from pathlib import Path
import pandas as pd
from py_load_pmda.parser import ApprovalsParser

def test_approvals_parser():
    parser = ApprovalsParser()
    file_path = Path("tests/fixtures/empty.xlsx")
    df = parser.parse(file_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "承認番号" in df.columns
    assert df["承認番号"][0] == "(302AMX00001000)"
    assert "販売名" in df.columns
    assert "申請者" in df.columns
    assert "承認日" in df.columns
