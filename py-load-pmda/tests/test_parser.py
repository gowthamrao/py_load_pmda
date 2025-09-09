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
