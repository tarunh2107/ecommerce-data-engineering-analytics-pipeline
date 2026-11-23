"""
Module: logger
Description: Standardized logging configuration for pipeline jobs and tasks.
"""
import logging
import sys


def get_logger(name: str = "ecommerce_pipeline") -> logging.Logger:
    """
    Returns a formatted logger instance writing to stdout.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
