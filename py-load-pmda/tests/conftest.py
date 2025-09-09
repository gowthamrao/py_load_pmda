import importlib.util

def pytest_ignore_collect(path, config):
    """
    Conditionally ignore test files if their dependencies are not installed.
    """
    if "test_redshift_adapter.py" in str(path):
        if not importlib.util.find_spec("redshift_connector"):
            return True  # Ignore this file
    return False
