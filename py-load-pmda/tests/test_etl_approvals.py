from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest
from requests_mock import Mocker

from py_load_pmda.extractor import ApprovalsExtractor
from py_load_pmda.transformer import ApprovalsTransformer


def test_approvals_extractor(requests_mock: Mocker, tmp_path: Path) -> None:
    """
    Tests the ApprovalsExtractor logic by mocking the multi-page navigation
    and download process.
    """
    # 1. Mock the initial landing page to find the link for the target year.
    # The extractor should be able to find the link to the 2025 page.
    requests_mock.get(
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010.html",
        text='<html><body><a href="/review-services/drug-reviews/review-information/p-drugs/2025_page.html">2025年度</a></body></html>',
    )

    # 2. Mock the 2025-specific page to find the link to the Excel file.
    # The link text "別表" (Appendix) should lead to the file download.
    requests_mock.get(
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/2025_page.html",
        text='<html><body><a href="/files/000276012.xlsx">別表</a></body></html>',
    )

    # 3. Mock the Excel file download itself.
    # Provide dummy content and an ETag for caching verification.
    file_content = b"dummy excel content"
    file_url = "https://www.pmda.go.jp/files/000276012.xlsx"
    requests_mock.get(file_url, content=file_content, headers={"ETag": "test-etag"})

    # --- Execute ---
    extractor = ApprovalsExtractor(cache_dir=str(tmp_path))
    # We pass an empty state, simulating the first run for this year.
    file_path, source_url, new_state = extractor.extract(year=2025, last_state={})

    # --- Assert ---
    # Verify the correct file was downloaded
    assert file_path.name == "000276012.xlsx"
    assert source_url == file_url
    assert file_path.read_bytes() == file_content

    # Verify the new state dictionary for caching
    assert "etag" in new_state
    assert new_state["etag"] == "test-etag"


@pytest.fixture
def sample_raw_df() -> pd.DataFrame:
    """Provides a sample raw DataFrame as output from the parser, now with Wareki dates."""
    data: Dict[str, List[Any]] = {
        "分野": ["第５", "抗悪"],
        "承認日": ["令和7年5月19日", "平成元年6月1日"],  # Wareki dates
        "No.": [1.0, 5.0],
        "販売名(会社名、法人番号)": [
            "スリンダ錠28\n(あすか製薬㈱、9010401018375)",
            "ブーレンレップ点滴静注用100 mg \n(グラクソ・スミスクライン㈱、2011001026329)",
        ],
        "承認": ["承　認", "承　認"],
        "成分名(下線:新有効成分)": ["ドロスピレノン", "ベランタマブ マホドチン（遺伝子組換え）"],
        "効能・効果等": [
            "避妊を効能・効果とする新効能・新用量・その他の医薬品",
            "再発又は難治性の多発性骨髄腫を効能・効果とする新有効成分含有医薬品\n【希少疾病用医薬品】",
        ],
    }
    return pd.DataFrame(data)


def test_approvals_transformer(sample_raw_df: pd.DataFrame) -> None:
    """Tests the ApprovalsTransformer logic, including Wareki date conversion."""
    source_url = "https://www.pmda.go.jp/files/000276012.xlsx"
    transformer = ApprovalsTransformer(source_url=source_url)

    transformed_df = transformer.transform([sample_raw_df])

    assert not transformed_df.empty
    assert "brand_name_jp" in transformed_df.columns
    assert "applicant_name_jp" in transformed_df.columns
    assert transformed_df.iloc[0]["brand_name_jp"] == "スリンダ錠28"
    assert transformed_df.iloc[0]["applicant_name_jp"] == "あすか製薬㈱"
    assert transformed_df.iloc[1]["brand_name_jp"] == "ブーレンレップ点滴静注用100 mg"
    assert transformed_df.iloc[1]["applicant_name_jp"] == "グラクソ・スミスクライン㈱"
    assert "_meta_source_url" in transformed_df.columns
    assert transformed_df.iloc[0]["_meta_source_url"] == source_url

    # Verify that the Wareki dates were correctly converted to date objects
    assert "approval_date" in transformed_df.columns
    assert transformed_df.iloc[0]["approval_date"] == date(2025, 5, 19)
    assert transformed_df.iloc[1]["approval_date"] == date(1989, 6, 1)
