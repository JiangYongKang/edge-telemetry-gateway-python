import logging
import sys

import pytest


@pytest.fixture(autouse=True)
def _log_to_stdout(caplog):
    """Make test logs (inputs & decision grounds) visible in pytest output."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("    [log] %(message)s"))
    with caplog.at_level(logging.INFO):
        caplog.handler.addFilter(logging.Filter())
        logging.getLogger("gateway-test").addHandler(handler)
        yield
        logging.getLogger("gateway-test").removeHandler(handler)
