import json
import logging
from logging import Formatter, LogRecord


class JSONFormatter(Formatter):
    """
    Formats log records as a JSON string.
    """

    def format(self, record: LogRecord) -> str:
        """
        Formats a log record into a JSON string.

        Args:
            record: The log record to format.

        Returns:
            A JSON string representing the log record.
        """
        log_object = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.name,
        }
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            log_object["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(log_object)


def setup_logging(level: str = "INFO") -> None:
    """
    Configures the root logger for the application.

    This function sets up a handler that outputs logs to the console,
    using the custom JSONFormatter. The log level is configurable.

    Args:
        level: The minimum logging level to output (e.g., "INFO", "DEBUG").
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Create a handler to write to stdout
    handler = logging.StreamHandler()
    handler.setLevel(log_level)

    # Create and set the custom JSON formatter
    formatter = JSONFormatter()
    handler.setFormatter(formatter)

    # Add the handler to the root logger
    logger.addHandler(handler)
