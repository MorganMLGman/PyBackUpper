import logging
import logging.config

logging.config.fileConfig("log_dev.conf")
logger = logging.getLogger('pybackupper_logger')