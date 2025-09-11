import json
import logging
from logging import Formatter, LogRecord
from typing import IO, Optional


class JSONFormatter(Formatter):
    """
    Formats log records as a JSON string.
    """

    def format(self, record: LogRecord) -> str:
        """
        Formats a log record into a JSON string.
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


def setup_logging(
    level: str = "INFO",
    log_format: str = "text",
    stream: Optional[IO[str]] = None,
    force: bool = False,
) -> None:
    """
    Configures the root logger for the application.
    This function is safe to call multiple times; it will not add duplicate handlers.

    Args:
        level: The minimum logging level to output.
        log_format: The format for logs ('text' or 'json').
        stream: The stream to log to. Defaults to sys.stdout.
        force: If True, will clear existing handlers and re-configure.
               Useful for testing.
    """
    logger = logging.getLogger()

    if logger.hasHandlers() and not force:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        return

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = logging.StreamHandler(stream)
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))

    if log_format.lower() == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)
