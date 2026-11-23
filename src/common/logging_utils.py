"""Standard logging configuration for the pipeline."""

import logging

# Same format already used by the three scripts (basicConfig is idempotent:
# the first call configures it, later calls change nothing)
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """Ensure the standard logging format and return a named logger."""
    logging.basicConfig(level=level, format=LOG_FORMAT)
    return logging.getLogger(name)
