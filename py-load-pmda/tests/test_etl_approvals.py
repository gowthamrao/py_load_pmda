import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import MagicMock

from py_load_pmda.extractor import ApprovalsExtractor
from py_load_pmda.parser import ApprovalsParser
from py_load_pmda.transformer import ApprovalsTransformer

@pytest.fixture
def mock_pmda_pages(mocker):
    """Mocks the requests.get calls to return fake PMDA HTML pages."""

    # Using a class to better structure the mock responses
    class MockResponse:
        def __init__(self, text="", status_code=200, headers=None, content=None, apparent_encoding="utf-8"):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}
            self._content = content or b""
            self.apparent_encoding = apparent_encoding
            self.encoding = None # Can be set by the calling code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise Exception("HTTP Error")

        def iter_content(self, chunk_size):
            yield self._content

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_responses = {
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010.html": MockResponse(
            text="""
            <html><body>
                <a href="/review-services/drug-reviews/review-information/p-drugs/0039.html">2025年度</a>
            </body></html>
            """
        ),
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0039.html": MockResponse(
            text="""
            <html><body>
                <a href="/files/000276012.xlsx">別表</a>
            </body></html>
            """
        ),
        "https://www.pmda.go.jp/files/000276012.xlsx": MockResponse(
            content=b"dummy excel content",
            headers={"ETag": "test-etag"}
        )
    }

    def get_side_effect(url, **kwargs):
        return mock_responses.get(url, MockResponse(status_code=404))

    mocker.patch("requests.get", side_effect=get_side_effect)

def test_approvals_extractor(mock_pmda_pages, tmp_path):
    """Tests the ApprovalsExtractor logic."""
    extractor = ApprovalsExtractor(cache_dir=str(tmp_path))
    file_path, source_url = extractor.extract(year=2025)

    assert file_path.name == "000276012.xlsx"
    assert source_url == "https://www.pmda.go.jp/files/000276012.xlsx"
    assert file_path.read_bytes() == b"dummy excel content"

@pytest.fixture
def sample_raw_df():
    """Provides a sample raw DataFrame as output from the parser."""
    data = {
        '分野': ['第５', '抗悪'],
        '承認日': ['2025.5.19', '2025.5.19'],
        'No.': [1.0, 5.0],
        '販売名(会社名、法人番号)': [
            'スリンダ錠28\n(あすか製薬㈱、9010401018375)',
            'ブーレンレップ点滴静注用100 mg \n(グラクソ・スミスクライン㈱、2011001026329)'
        ],
        '承認': ['承　認', '承　認'],
        '成分名(下線:新有効成分)': ['ドロスピレノン', 'ベランタマブ マホドチン（遺伝子組換え）'],
        '効能・効果等': ['避妊を効能・効果とする新効能・新用量・その他の医薬品', '再発又は難治性の多発性骨髄腫を効能・効果とする新有効成分含有医薬品\n【希少疾病用医薬品】']
    }
    return pd.DataFrame(data)

def test_approvals_transformer(sample_raw_df):
    """Tests the ApprovalsTransformer logic."""
    source_url = "https://www.pmda.go.jp/files/000276012.xlsx"
    transformer = ApprovalsTransformer(source_url=source_url)

    transformed_df = transformer.transform(sample_raw_df)

    assert not transformed_df.empty
    assert 'brand_name_jp' in transformed_df.columns
    assert 'applicant_name_jp' in transformed_df.columns
    assert transformed_df.iloc[0]['brand_name_jp'] == 'スリンダ錠28'
    assert transformed_df.iloc[0]['applicant_name_jp'] == 'あすか製薬㈱'
    assert transformed_df.iloc[1]['brand_name_jp'] == 'ブーレンレップ点滴静注用100 mg'
    assert transformed_df.iloc[1]['applicant_name_jp'] == 'グラクソ・スミスクライン㈱'
    assert '_meta_source_url' in transformed_df.columns
    assert transformed_df.iloc[0]['_meta_source_url'] == source_url
