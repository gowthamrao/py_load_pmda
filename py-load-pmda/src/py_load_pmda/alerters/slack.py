import logging
from typing import Any, Dict

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from py_load_pmda.alerters.base import Alerter

logger = logging.getLogger(__name__)


class SlackAlerter(Alerter):
    """
    An alerter that sends messages to a Slack channel.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the SlackAlerter.

        Args:
            config: A dictionary containing the 'token' and 'channel'.
        """
        try:
            self.token = config["token"]
            self.channel = config["channel"]
            self.client = WebClient(token=self.token)
        except KeyError as e:
            raise ValueError(f"Missing required config key for SlackAlerter: {e}")

    def send(self, message: str, subject: str = "Pipeline Alert") -> None:
        """
        Sends a message to the configured Slack channel.

        Args:
            message: The content of the alert message.
            subject: The subject or title of the alert (used as a header).
        """
        try:
            full_message = f"*{subject}*\n\n{message}"
            response = self.client.chat_postMessage(
                channel=self.channel, text=full_message, mrkdwn=True
            )
            assert response["ok"]
            logger.info(f"Successfully sent alert to Slack channel '{self.channel}'.")
        except SlackApiError as e:
            logger.error(f"Failed to send Slack alert: {e.response['error']}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending Slack alert: {e}")
