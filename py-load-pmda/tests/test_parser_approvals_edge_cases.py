import pytest
import pandas as pd
from pathlib import Path
from py_load_pmda.parser import ApprovalsParser
import openpyxl

@pytest.fixture
def approvals_parser() -> ApprovalsParser:
    """Fixture to create an ApprovalsParser instance."""
    return ApprovalsParser()

def test_parse_excel_no_header(approvals_parser: ApprovalsParser, tmp_path: Path):
    """
    Test that a ValueError is raised if the header row is not found.
    """
    file_path = tmp_path / "test.xlsx"
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df.to_excel(file_path, index=False, header=False)
    with pytest.raises(ValueError, match="Could not find header row containing '販売名'"):
        approvals_parser.parse(file_path)

def test_parse_empty_excel(approvals_parser: ApprovalsParser, tmp_path: Path):
    """
    Test that an empty list is returned when parsing an empty excel file.
    """
    file_path = tmp_path / "test.xlsx"
    file_path.touch()
    with pytest.raises(ValueError, match="Excel file format cannot be determined, you must specify an engine manually."):
        approvals_parser.parse(file_path)


def test_parse_corrupted_excel(approvals_parser: ApprovalsParser, tmp_path: Path):
    """
    Test that an exception is raised when parsing a corrupted excel file.
    """
    file_path = tmp_path / "test.xlsx"
    with open(file_path, "w") as f:
        f.write("this is not an excel file")
    with pytest.raises(Exception):
        approvals_parser.parse(file_path)

def test_parse_excel_different_sheet_name(approvals_parser: ApprovalsParser, tmp_path: Path):
    """
    Test that the parser can handle an excel file with a different sheet name.
    """
    file_path = tmp_path / "test.xlsx"
    df = pd.DataFrame({
        "No.": [1, 2],
        "申請区分": ["区分1", "区分2"],
        "販売名": ["test1", "test2"],
        "一般名": ["generic1", "generic2"],
        "申請者名": ["applicant1", "applicant2"],
        "承認年月日": ["2025/01/01", "2025/01/02"],
        "備考": ["", ""]
    })
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name="another_sheet", index=False)

    parsed_data = approvals_parser.parse(file_path)
    assert len(parsed_data) == 1
    assert not parsed_data[0].empty
    assert "brand_name_jp" in parsed_data[0].columns
