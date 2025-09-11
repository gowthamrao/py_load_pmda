import logging

from py_load_pmda.alerters.base import Alerter


class LogAlerter(Alerter):
    """A simple alerter that writes messages to the log."""

    def send(self, message: str, subject: str = "Pipeline Alert") -> None:
        """
        Logs the alert message at the ERROR level.

        Args:
            message: The content of the alert message.
            subject: The subject or title of the alert.
        """
        logging.error(f"ALERT - {subject}: {message}")
