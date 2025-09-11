import pytest
from py_load_pmda.extractor import ApprovalsExtractor

@pytest.fixture
def mock_pmda_pages(requests_mock):
    requests_mock.get(
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010.html",
        text='<html><body><a href="/review-services/drug-reviews/review-information/p-drugs/0011.html">2025年度</a></body></html>'
    )
    requests_mock.get(
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0011.html",
        text='<html><body><a href="/drugs/2025/P001/01_1.xlsx">Excel Link</a></body></html>'
    )
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/2025/P001/01_1.xlsx",
        content=b"dummy content"
    )

def test_approvals_extractor(mock_pmda_pages):
    extractor = ApprovalsExtractor()
    files = extractor.extract(2025)
    assert len(files) == 1
    assert files[0].name.endswith(".xlsx")
