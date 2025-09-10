import unittest

from py_load_pmda.parser import BasePDFParser, PackageInsertsParser, ReviewReportsParser


class TestParserRefactoring(unittest.TestCase):

    def test_inheritance(self):
        """
        Tests that the PDF parser classes inherit from the base class.
        """
        self.assertTrue(issubclass(PackageInsertsParser, BasePDFParser))
        self.assertTrue(issubclass(ReviewReportsParser, BasePDFParser))

    def test_parse_method_existence(self):
        """
        Tests that the parser instances have the 'parse' method.
        """
        package_parser = PackageInsertsParser()
        review_parser = ReviewReportsParser()
        self.assertTrue(hasattr(package_parser, 'parse'))
        self.assertTrue(hasattr(review_parser, 'parse'))
        self.assertTrue(callable(package_parser.parse))
        self.assertTrue(callable(review_parser.parse))


from pathlib import Path
import pandas as pd
import pytest

from py_load_pmda.parser import XMLParser


def test_xml_parser_success():
    """
    Tests that the XMLParser can successfully parse a valid XML file
    using a given XPath.
    """
    # Arrange
    parser = XMLParser()
    # Assuming the test is run from the root of the 'py-load-pmda' directory
    fixture_path = Path("tests/fixtures/pmda_test_report.xml")
    xpath_expr = "./products/product"

    # Act
    result_df = parser.parse(fixture_path, xpath=xpath_expr)

    # Assert
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 3
    # Use sets to make the column check order-independent
    # The 'approved' attribute is on a child node, so it is not parsed as a column.
    assert set(result_df.columns) == {"id", "name", "category", "status"}

    # Check content of the first row
    assert result_df.iloc[0]["id"] == "A123"
    assert result_df.iloc[0]["name"] == "DrugA"
    assert result_df.iloc[0]["status"] == "Approved"

    # Check content of the second row to be sure
    assert result_df.iloc[1]["id"] == "B456"
    assert result_df.iloc[1]["name"] == "DeviceB"
    assert result_df.iloc[1]["status"] == "Pending"

def test_xml_parser_file_not_found():
    """
    Tests that the XMLParser raises a FileNotFoundError for a non-existent file.
    """
    # Arrange
    parser = XMLParser()
    non_existent_path = Path("non/existent/file.xml")

    # Act & Assert
    with pytest.raises(FileNotFoundError):
        parser.parse(non_existent_path, xpath="./*")


def test_xml_parser_invalid_xpath():
    """
    Tests that the XMLParser handles cases where the XPath finds no nodes.
    pandas.read_xml raises a ValueError in this case.
    """
    # Arrange
    parser = XMLParser()
    fixture_path = Path("tests/fixtures/pmda_test_report.xml")
    invalid_xpath = "./nonexistent/path"

    # Act & Assert
    # Check for the exception type only, not the message, to make the test less brittle.
    with pytest.raises(ValueError):
        parser.parse(fixture_path, xpath=invalid_xpath)

def test_xml_parser_no_xpath_provided():
    """
    Tests that the XMLParser raises a ValueError if no XPath is provided.
    """
    # Arrange
    parser = XMLParser()
    fixture_path = Path("tests/fixtures/pmda_test_report.xml")

    # Act & Assert
    with pytest.raises(ValueError, match="An XPath expression must be provided."):
        parser.parse(fixture_path, xpath="")
