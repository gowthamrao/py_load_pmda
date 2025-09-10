import logging
from unittest.mock import MagicMock, patch

import pytest

from py_load_pmda.alerting import AlertManager, LogAlerter


def test_log_alerter(caplog):
    """Test that the LogAlerter calls logging.error."""
    alerter = LogAlerter()
    with caplog.at_level(logging.ERROR):
        alerter.send("Test message", subject="Test Subject")

    assert "ALERT - Test Subject: Test message" in caplog.text


def test_alert_manager_no_config():
    """Test that the AlertManager does nothing with no alerters configured."""
    manager = AlertManager(config=[])
    # We can't easily assert that nothing happens, but we can run it
    # to ensure it doesn't crash.
    manager.send("Test message")


def test_alert_manager_with_log_alerter():
    """Test that the AlertManager correctly uses the LogAlerter."""
    with patch("py_load_pmda.alerting.LogAlerter") as mock_log_alerter_class:
        # Arrange
        mock_alerter_instance = MagicMock()
        mock_log_alerter_class.return_value = mock_alerter_instance
        config = [{"type": "log"}]

        # Act
        manager = AlertManager(config)
        manager.send("Test message", subject="Important Alert")

        # Assert
        mock_log_alerter_class.assert_called_once()
        mock_alerter_instance.send.assert_called_once_with("Test message", subject="Important Alert")


def test_alert_manager_unknown_alerter(caplog):
    """Test that the AlertManager handles unknown alerter types gracefully."""
    with caplog.at_level(logging.WARNING):
        config = [{"type": "email", "to": "test@example.com"}]
        manager = AlertManager(config)
        assert "Unknown alerter type 'email' found in config" in caplog.text
        # Ensure no alerters were actually added
        assert not manager.alerters


def test_alert_manager_alerter_fails():
    """Test that the AlertManager handles failures in an alerter's send method."""
    with patch("py_load_pmda.alerting.LogAlerter") as mock_log_alerter_class:
        # Arrange
        mock_alerter_instance = MagicMock()
        mock_alerter_instance.send.side_effect = Exception("SMTP server down")
        mock_log_alerter_class.return_value = mock_alerter_instance

        config = [{"type": "log"}]
        manager = AlertManager(config)

        # Act
        # We expect it to log the error but not crash
        with patch.object(logging, 'error') as mock_log_error:
            manager.send("Test message")

            # Assert
            assert "Failed to send alert" in mock_log_error.call_args[0][0]
