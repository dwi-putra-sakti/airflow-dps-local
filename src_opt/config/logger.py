"""
src_opt.config.logger
Standardized logger utility for Airflow tasks and pipeline jobs.
"""
import logging
import sys


def get_pipeline_logger(name: str = "airflow_pipeline") -> logging.Logger:
    """Return a configured logger matching Airflow worker logging formats."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger
