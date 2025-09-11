from abc import ABC, abstractmethod


class Alerter(ABC):
    """Abstract base class for all alerter implementations."""

    @abstractmethod
    def send(self, message: str, subject: str = "Pipeline Alert") -> None:
        """
        Sends an alert.

        Args:
            message: The content of the alert message.
            subject: The subject or title of the alert.
        """
        pass
