# src/utils/logger.py

import logging


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Create and return a configured logger.

    Args:
        name (str): logger name
        level (int): logging level (default INFO)

    Returns:
        logging.Logger
    """

    logger = logging.getLogger(name)

    if not logger.handlers:

        logger.setLevel(level)

        handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        handler.setFormatter(formatter)

        logger.addHandler(handler)

        logger.propagate = False

    return logger