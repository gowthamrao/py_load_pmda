import pandas as pd
import pytest
from py_load_pmda.transformer import convert_wareki_to_ad, ApprovalsTransformer

def test_convert_wareki_to_ad():
    assert convert_wareki_to_ad("令和7年9月8日") == pd.Timestamp("2025-09-08")
    assert convert_wareki_to_ad("平成31年4月30日") == pd.Timestamp("2019-04-30")
    assert convert_wareki_to_ad("昭和64年1月7日") == pd.Timestamp("1989-01-07")
    assert convert_wareki_to_ad("Invalid Date") is None
    assert convert_wareki_to_ad("") is None
    assert convert_wareki_to_ad(None) is None

def test_approvals_transformer():
    transformer = ApprovalsTransformer()
    data = {
        "承認番号": ["(302AMX00001000)"],
        "申請区分": ["申請区分"],
        "販売名": ["テストメディカル"],
        "一般名": ["一般名"],
        "申請者": ["テスト製薬株式会社"],
        "承認日": ["令和7年9月8日"],
        "効能・効果": ["効能・効果"],
        "審査報告書": ["http://example.com"],
        "_meta_source_file": ["test.xlsx"],
    }
    df = pd.DataFrame(data)
    transformed_df = transformer.transform(df)
    assert len(transformed_df) == 1
    assert transformed_df["approval_id"][0] == "(302AMX00001000)"
    assert transformed_df["approval_date"][0] == pd.Timestamp("2025-09-08")
    assert "_meta_load_ts_utc" in transformed_df.columns
    assert "_meta_source_content_hash" in transformed_df.columns
    assert "raw_data_full" in transformed_df.columns
