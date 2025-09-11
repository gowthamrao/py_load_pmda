import io
import json
import logging
import unittest

from py_load_pmda.logging_config import setup_logging


class TestLogging(unittest.TestCase):
    def setUp(self):
        """
        Set up a stream to capture log output for each test.
        """
        self.log_stream = io.StringIO()

    def tearDown(self):
        """
        Clean up by clearing handlers after each test to ensure isolation.
        """
        logging.getLogger().handlers.clear()

    def test_default_text_logging(self):
        """
        Test that the default logging format is plain text.
        """
        # Use force=True to ensure our test handler is applied
        setup_logging(level="INFO", stream=self.log_stream, force=True)

        # Log a message
        test_message = "This is a default format test."
        logging.info(test_message)

        # Get the log output
        log_output = self.log_stream.getvalue()

        # Assert that the output is plain text and contains the message
        self.assertNotIn("{", log_output)
        self.assertIn("INFO", log_output)
        self.assertIn(test_message, log_output)

    def test_json_logging(self):
        """
        Test that logging with format='json' produces valid JSON.
        """
        # Use force=True to ensure our test handler is applied
        setup_logging(level="DEBUG", log_format="json", stream=self.log_stream, force=True)

        # Log a message
        test_message = "This is a JSON format test."
        logging.debug(test_message)

        # Get the log output
        log_output = self.log_stream.getvalue().strip()

        # Assert that the output is a valid JSON string
        try:
            log_data = json.loads(log_output)
            self.assertIsInstance(log_data, dict)
        except json.JSONDecodeError:
            self.fail("Logging output was not valid JSON.")

        # Assert that the JSON object contains the expected keys and values
        self.assertEqual(log_data.get("level"), "DEBUG")
        self.assertEqual(log_data.get("message"), test_message)
        self.assertIn("timestamp", log_data)
        self.assertEqual(log_data.get("module"), "root")

    def test_text_logging_explicit(self):
        """
        Test that logging with format='text' produces plain text.
        """
        # Use force=True to ensure our test handler is applied
        setup_logging(level="INFO", log_format="text", stream=self.log_stream, force=True)

        test_message = "This is an explicit text test."
        logging.info(test_message)

        log_output = self.log_stream.getvalue()

        self.assertNotIn("{", log_output)
        self.assertIn("INFO", log_output)
        self.assertIn(test_message, log_output)


if __name__ == "__main__":
    unittest.main()
