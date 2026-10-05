"""
src_opt.common.utils
Common utility functions, date helpers, and execution timers.
"""
import time
from contextlib import contextmanager
from datetime import datetime, date
from typing import Generator
import pendulum

from src_opt.config.settings import settings
from src_opt.config.logger import get_pipeline_logger

logger = get_pipeline_logger("src_opt.common.utils")


def get_local_now() -> datetime:
    """Return current localized datetime in Asia/Jakarta."""
    return pendulum.now(settings.TIMEZONE)


def format_duration(seconds: float) -> str:
    """Format execution duration in human-readable string."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    rem_sec = seconds % 60
    return f"{minutes}m {rem_sec:.1f}s"


@contextmanager
def execution_timer(task_name: str) -> Generator[None, None, None]:
    """Context manager to measure and log execution time of a task."""
    start_time = time.time()
    logger.info(f"▶ Starting task: [{task_name}] at {get_local_now().to_datetime_string()}")
    try:
        yield
    except Exception as exc:
        duration = time.time() - start_time
        if exc.__class__.__name__ == "AirflowSkipException":
            logger.info(f"⏭ Task [{task_name}] skipped after {format_duration(duration)}")
            raise
        logger.error(f"❌ Task [{task_name}] failed after {format_duration(duration)}: {exc}", exc_info=True)
        raise
    else:
        duration = time.time() - start_time
        logger.info(f"✔ Completed task: [{task_name}] in {format_duration(duration)}")

