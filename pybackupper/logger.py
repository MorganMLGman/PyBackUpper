import logging
import logging.config
from pathlib import Path

logging.config.fileConfig("pybackupper/log_dev.conf")
logger = logging.getLogger('pybackupper_logger')

def get_last_log_file_path() -> Path:
    """Get the last log file from the logs directory.

    Returns:
        str: Path to the last log file.
    """
    log_dir = "logs/"
    log_files = sorted(Path(log_dir).glob("*.log"), key=lambda x: x.stat().st_mtime, reverse=True)
    if log_files:
        return log_files[0].resolve()
    else:
        logger.warning("No log files found.")
        return None