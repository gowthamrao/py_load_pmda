import unittest
from unittest.mock import patch, MagicMock

from py_load_pmda.alerters.slack import SlackAlerter
from slack_sdk.errors import SlackApiError


class TestSlackAlerter(unittest.TestCase):
    def test_init_success(self):
        config = {"token": "test-token", "channel": "test-channel"}
        alerter = SlackAlerter(config)
        self.assertEqual(alerter.token, "test-token")
        self.assertEqual(alerter.channel, "test-channel")
        self.assertIsNotNone(alerter.client)

    def test_init_missing_key(self):
        with self.assertRaises(ValueError):
            SlackAlerter({"token": "test-token"})

    @patch("slack_sdk.WebClient")
    def test_send_success(self, mock_web_client):
        mock_client_instance = mock_web_client.return_value
        mock_client_instance.chat_postMessage.return_value = {"ok": True}

        config = {"token": "test-token", "channel": "test-channel"}
        alerter = SlackAlerter(config)
        alerter.client = mock_client_instance

        alerter.send("Test message", subject="Test Subject")

        mock_client_instance.chat_postMessage.assert_called_with(
            channel="test-channel",
            text="*Test Subject*\n\nTest message",
            mrkdwn=True
        )

    @patch("slack_sdk.WebClient")
    def test_send_slack_api_error(self, mock_web_client):
        mock_client_instance = mock_web_client.return_value
        mock_client_instance.chat_postMessage.side_effect = SlackApiError(
            "API error", {"ok": False, "error": "test_error"}
        )

        config = {"token": "test-token", "channel": "test-channel"}
        alerter = SlackAlerter(config)
        alerter.client = mock_client_instance

        with self.assertLogs('py_load_pmda.alerters.slack', level='ERROR') as cm:
            alerter.send("Test message")
            self.assertIn("Failed to send Slack alert: test_error", cm.output[0])

    @patch("slack_sdk.WebClient")
    def test_send_unexpected_error(self, mock_web_client):
        mock_client_instance = mock_web_client.return_value
        mock_client_instance.chat_postMessage.side_effect = Exception("Unexpected error")

        config = {"token": "test-token", "channel": "test-channel"}
        alerter = SlackAlerter(config)
        alerter.client = mock_client_instance

        with self.assertLogs('py_load_pmda.alerters.slack', level='ERROR') as cm:
            alerter.send("Test message")
            self.assertIn("An unexpected error occurred while sending Slack alert: Unexpected error", cm.output[0])

if __name__ == "__main__":
    unittest.main()
