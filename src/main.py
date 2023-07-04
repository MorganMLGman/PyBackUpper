"""PyBackUpper main module.

Raises:
    ValueError: Exception raised when required environment variable has invalid value.
    KeyError: Exception raised required environment variable is not set.
"""

import logging
import logging.config
import os
from s3_handler import S3Handler
from telegram_handler import TelegramHandler
from backups_manager import BackupManager
from pprint import pformat

class PyBackUpper():
    """PyBackUpper class.
    """
    def __init__(self, logger:logging.Logger=None):
        """PyBackUpper constructor.

        Args:
            logger (logging.Logger, optional): Logger to use. Defaults to None.
        """
        if logger is None:
            logging.config.fileConfig("log.conf")
            self.logger = logging.getLogger('pybackupper_logger')
        else:
            self.logger = logger
        self.logger.info("PyBackUpper initialized.")
        self.config = {}
        self.read_env()
        
        if self.config["S3_BUCKET"] is not None and self.config["S3_ACCESS_KEY_ID"] is not None and self.config["S3_SECRET_ACCESS_KEY"] is not None:
            self.s3_handler = S3Handler(
                self.config["S3_BUCKET"], 
                self.config["S3_ACCESS_KEY_ID"], 
                self.config["S3_SECRET_ACCESS_KEY"], 
                self.config["S3_ACL"] if self.config["S3_ACL"] is not None else 'public-read',
                self.config["S3_REGION_NAME"] if self.config["S3_REGION_NAME"] is not None else 'us-east-1',
                self.config["S3_ENDPOINT_URL"] if self.config["S3_ENDPOINT_URL"] is not None else 'https://s3.amazonaws.com',
                logger=self.logger)
            if not self.s3_handler.test_connection():
                self.logger.error("S3 connection test failed. S3 upload will not be available.")
                self.s3_handler = None
            else:
                self.logger.info("S3 connection test successful.")
            
        else:
            self.logger.warning("S3_BUCKET, S3_ACCESS_KEY and S3_SECRET_KEY not set. S3 upload will not be available.")
            self.s3_handler = None
        
        if self.config["TELEGRAM_TOKEN"] is not None and self.config["TELEGRAM_CHAT_ID"] is not None:
            self.telegram_handler = TelegramHandler(self.config["TELEGRAM_TOKEN"], self.config["TELEGRAM_CHAT_ID"], logger=self.logger)
            if not self.telegram_handler.test_connection():
                self.logger.error("Telegram connection test failed. Telegram notifications will not be available.")
                self.telegram_handler = None
            else:
                self.logger.info("Telegram connection test successful.")
        else:
            self.logger.warning("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID not set. Telegram notifications will not be available.")
            
        self.backups_manager = BackupManager(
            logger=self.logger,
            s3handler = self.s3_handler,
            is_compression_enabled = self.config["COMPRESSION_ENABLED"],
            archive_format = self.config["ARCHIVE_FORMAT"],
            raw_backup_keep = self.config["LOCAL_RAW_BACKUPS_KEEP"],
            compressed_backup_keep = self.config["LOCAL_COMPRESSED_BACKUPS_KEEP"],
            s3_raw_keep = self.config["S3_RAW_BACKUPS_KEEP"],
            s3_compressed_keep = self.config["S3_COMPRESSED_BACKUPS_KEEP"],
            ignored_extensions = self.config["IGNORED_EXTENSIONS"]
        )
        
#         if self.telegram_handler:
#             self.telegram_handler.send_message(f"\
# PyBackUpper initialized.\n\n\
# Config:\n\
# `{self.print_config()}`")
        
    def read_env(self):
        """Reads environment variables and stores them in self.config.

        Raises:
            ValueError: Exception raised when required environment variable has invalid value.
            KeyError: Exception raised required environment variable is not set.
        """
        self.logger.info("Reading environment variables.")
        
        try:
            self.config["HOSTNAME"] = os.environ['HOSTNAME'].strip().replace('"', '')
        except KeyError as e:
            raise KeyError("HOSTNAME not set.") from e
        
        try:
            self.config["PUID"] = int(os.environ['PUID'].strip().replace('"', ''))
            if self.config["PUID"] < 0 or self.config["PUID"] > 65535:
                self.logger.error("Value of PUID must be between 0 and 65535, not %s", self.config["PUID"])
                raise ValueError("Value of PUID must be between 0 and 65535")
        except KeyError as e:
            raise KeyError("PUID not set.") from e
            
        try:
            self.config["PGID"] = int(os.environ['PGID'].strip().replace('"', ''))
            if self.config["PGID"] < 0 or self.config["PGID"] > 65535:
                self.logger.error("Value of PGID must be between 0 and 65535, not %s", self.config["PGID"])
                raise ValueError("Value of PGID must be between 0 and 65535")
        except KeyError as e:
            raise KeyError("PGID not set.") from e
            
        try:
            self.config["DAYS_TO_RUN"] = [int(x) for x in os.environ['DAYS_TO_RUN'].strip().replace('"', '').split(',')]
            for day in self.config["DAYS_TO_RUN"]:
                if day < 0 or day > 6:
                    self.logger.error("Value of DAYS_TO_RUN must be between 0 and 6, not %s", day)
                    raise ValueError("Value of DAYS_TO_RUN must be between 0 and 6")
                
            if len(self.config["DAYS_TO_RUN"]) != len(set(self.config["DAYS_TO_RUN"])):
                self.logger.error("DAYS_TO_RUN contains duplicates.")
                raise ValueError("DAYS_TO_RUN contains duplicates")
        
        except KeyError as e:
            raise KeyError("DAYS_TO_RUN not set.") from e
            
        try:
            self.config["HOUR"] = int(os.environ['HOUR'].strip().replace('"', ''))
            if self.config["HOUR"] < 0 or self.config["HOUR"] > 23:
                self.logger.error("Value of HOUR must be between 0 and 23, not %s", self.config["HOUR"])
                raise ValueError("Value of HOUR must be between 0 and 23")
        except KeyError as e:
            raise KeyError("HOUR not set.") from e
            
        try:
            self.config["MINUTE"] = int(os.environ['MINUTE'].strip().replace('"', ''))
            if self.config["MINUTE"] < 0 or self.config["MINUTE"] > 59:
                self.logger.error("Value of MINUTE must be between 0 and 59, not %s", self.config["MINUTE"])
                raise ValueError("Value of MINUTE must be between 0 and 59")
        except KeyError as e:
            raise KeyError("MINUTE not set.") from e
            
        try:
            if os.environ['COMPRESSION_ENABLED'].strip().replace('"', '').lower() == "true":
                self.config["COMPRESSION_ENABLED"] = True
            elif os.environ['COMPRESSION_ENABLED'].strip().replace('"', '').lower() == "false":
                self.config["COMPRESSION_ENABLED"] = False
            else:
                self.logger.error("COMPRESSION_ENABLED must be either true or false.")
                raise ValueError("COMPRESSION_ENABLED must be either true or false")
        except KeyError:
            self.logger.warning("COMPRESSION_ENABLED not set. Defaulting to true.")
            self.config["COMPRESSION_ENABLED"] = True
            
        if self.config["COMPRESSION_ENABLED"]:
            try:
                self.config["ARCHIVE_FORMAT"] = os.environ['ARCHIVE_FORMAT'].strip().replace('"', '')
                if self.config["ARCHIVE_FORMAT"] not in ["tar", "tar.gz", "tar.bz2", "tar.xz", "zip"]:
                    self.logger.error("ARCHIVE_FORMAT must be one of tar, tar.gz, tar.bz2, tar.xz, zip, not %s", self.config["ARCHIVE_FORMAT"])
                    raise ValueError("ARCHIVE_FORMAT must be one of tar, tar.gz, tar.bz2, tar.xz, zip")
            except KeyError:
                self.logger.warning("ARCHIVE_FORMAT not set. Defaulting to tar.gz.")
                self.config["ARCHIVE_FORMAT"] = "tar.gz"
        else:
            self.config["ARCHIVE_FORMAT"] = None
            
        try:
            self.config["LOCAL_RAW_BACKUPS_KEEP"] = int(os.environ['LOCAL_RAW_BACKUPS_KEEP'].strip().replace('"', ''))
            if self.config["LOCAL_RAW_BACKUPS_KEEP"] < 0:
                self.logger.error("Value of LOCAL_RAW_BACKUPS_KEEP must be at least 0, not %s", self.config["LOCAL_RAW_BACKUPS_KEEP"])
                raise ValueError("Value of LOCAL_RAW_BACKUPS_KEEP must be at least 0")
        except KeyError:
            self.logger.warning("LOCAL_RAW_BACKUPS_KEEP not set. Defaulting to 1.")
            self.config["LOCAL_RAW_BACKUPS_KEEP"] = 1
            
        if self.config["COMPRESSION_ENABLED"]:
            try:
                self.config["LOCAL_COMPRESSED_BACKUPS_KEEP"] = int(os.environ['LOCAL_COMPRESSED_BACKUPS_KEEP'].strip().replace('"', ''))
                if self.config["LOCAL_COMPRESSED_BACKUPS_KEEP"] < 0:
                    self.logger.error("Value of LOCAL_COMPRESSED_BACKUPS_KEEP must be at least 0, not %s", self.config["LOCAL_COMPRESSED_BACKUPS_KEEP"])
                    raise ValueError("Value of LOCAL_COMPRESSED_BACKUPS_KEEP must be at least 0")
            except KeyError:
                self.logger.warning("LOCAL_COMPRESSED_BACKUPS_KEEP not set. Defaulting to 1.")
                self.config["LOCAL_COMPRESSED_BACKUPS_KEEP"] = 1
        else:
            self.config["LOCAL_COMPRESSED_BACKUPS_KEEP"] = 0
            
        try:
            self.config["S3_RAW_BACKUPS_KEEP"] = int(os.environ['S3_RAW_BACKUPS_KEEP'].strip().replace('"', ''))
            if self.config["S3_RAW_BACKUPS_KEEP"] < 0:
                self.logger.error("Value of S3_RAW_BACKUPS_KEEP must be at least 0, not %s", self.config["S3_RAW_BACKUPS_KEEP"])
                raise ValueError("Value of S3_RAW_BACKUPS_KEEP must be at least 0")
        except KeyError:
            self.logger.warning("S3_RAW_BACKUPS_KEEP not set. Defaulting to 0.")
            self.config["S3_RAW_BACKUPS_KEEP"] = 0
            
        if self.config["COMPRESSION_ENABLED"]:
            try:
                self.config["S3_COMPRESSED_BACKUPS_KEEP"] = int(os.environ['S3_COMPRESSED_BACKUPS_KEEP'].strip().replace('"', ''))
                if self.config["S3_COMPRESSED_BACKUPS_KEEP"] < 0:
                    self.logger.error("Value of S3_COMPRESSED_BACKUPS_KEEP must be at least 0, not %s", self.config["S3_COMPRESSED_BACKUPS_KEEP"])
                    raise ValueError("Value of S3_COMPRESSED_BACKUPS_KEEP must be at least 0")
            except KeyError:
                self.logger.warning("S3_COMPRESSED_BACKUPS_KEEP not set. Defaulting to 0.")
                self.config["S3_COMPRESSED_BACKUPS_KEEP"] = 0
        else:
            self.config["S3_COMPRESSED_BACKUPS_KEEP"] = 0
        
        try:
            self.config["S3_BUCKET"] = os.environ['S3_BUCKET'].strip().replace('"', '')
        except KeyError as e:
            self.logger.warning("S3_BUCKET not set.")
            self.config["S3_BUCKET"] = None
            
        try:
            self.config["S3_ENDPOINT_URL"] = os.environ['S3_ENDPOINT_URL'].strip().replace('"', '')
        except KeyError:
            self.logger.warning("S3_ENDPOINT_URL not set. Defaulting to None.")
            self.config["S3_ENDPOINT_URL"] = None
            
            
        try:
            with open(os.environ['S3_ACCESS_KEY_ID_FILE'].strip().replace('"', ''), 'r') as f:
                self.config["S3_ACCESS_KEY_ID"] = f.read().strip()
        except (FileNotFoundError, KeyError):
            try:
                self.config["S3_ACCESS_KEY_ID"] = os.environ['S3_ACCESS_KEY_ID'].strip().replace('"', '')
            except KeyError:
                self.logger.warning("S3_ACCESS_KEY_ID not set. Defaulting to None.")
                self.config["S3_ACCESS_KEY_ID"] = None
        
        try:
            with open(os.environ['S3_SECRET_ACCESS_KEY_FILE'].strip().replace('"', ''), 'r') as f:
                self.config["S3_SECRET_ACCESS_KEY"] = f.read().strip()
        except (FileNotFoundError, KeyError) as e:
            print(e)
            try:
                self.config["S3_SECRET_ACCESS_KEY"] = os.environ['S3_SECRET_ACCESS_KEY'].strip().replace('"', '')
            except KeyError:
                self.logger.warning("S3_SECRET_ACCESS_KEY not set. Defaulting to None.")
                self.config["S3_SECRET_ACCESS_KEY"] = None
            
        try:
            self.config["S3_REGION_NAME"] = os.environ['S3_REGION_NAME'].strip().replace('"', '')
        except KeyError:
            self.logger.warning("S3_REGION_NAME not set. Defaulting to None.")
            self.config["S3_REGION_NAME"] = None
        
        try:
            self.config["S3_ACL"] = os.environ['S3_ACL'].strip().replace('"', '')
        except KeyError:
            self.logger.warning("S3_ACL not set. Defaulting to None.")
            self.config["S3_ACL"] = None
            
        try:
            self.config["IGNORED_EXTENSIONS"] = os.environ['IGNORED_EXTENSIONS'].strip().replace('"', '')
            if self.config["IGNORED_EXTENSIONS"] == "":
                self.config["IGNORED_EXTENSIONS"] = []
            else:
                self.config["IGNORED_EXTENSIONS"] = self.config["IGNORED_EXTENSIONS"].split(",")
        except KeyError:
            self.config["IGNORED_EXTENSIONS"] = []
      
        try:
            with open(os.environ['TELEGRAM_TOKEN_FILE'].strip().replace('"', ''), 'r') as f:
                self.config["TELEGRAM_TOKEN"] = f.read().strip()
        except (FileNotFoundError, KeyError):      
            try:
                self.config["TELEGRAM_TOKEN"] = os.environ['TELEGRAM_TOKEN'].strip().replace('"', '')
            except KeyError:
                self.logger.warning("TELEGRAM_TOKEN not set. Telegram notifications disabled.")
                self.config["TELEGRAM_TOKEN"] = None
                
        try:
            with open(os.environ['TELEGRAM_CHAT_ID_FILE'].strip().replace('"', ''), 'r') as f:
                self.config["TELEGRAM_CHAT_ID"] = f.read().strip()
        except (FileNotFoundError, KeyError):   
            try:
                self.config["TELEGRAM_CHAT_ID"] = os.environ['TELEGRAM_CHAT_ID'].strip().replace('"', '')
            except KeyError:
                self.logger.warning("TELEGRAM_CHAT_ID not set. Telegram notifications disabled.")
                self.config["TELEGRAM_CHAT_ID"] = None
    
    def print_config(self) -> str:
        config = self.config.copy()
        
        if config["S3_ACCESS_KEY_ID"] is not None:
            config["S3_ACCESS_KEY_ID"] = "********"
            
        if config["S3_SECRET_ACCESS_KEY"] is not None:
            config["S3_SECRET_ACCESS_KEY"] = "********"
            
        if config["TELEGRAM_TOKEN"] is not None:
            config["TELEGRAM_TOKEN"] = "********"
        
        if config["TELEGRAM_CHAT_ID"] is not None:
            config["TELEGRAM_CHAT_ID"] = "********"
            
        return pformat(config, sort_dicts=False)
        
            
if __name__ == "__main__":
    pybackupper = PyBackUpper()
    try:
        response = pybackupper.backups_manager.perform_backup()
        if response is not None and response != "":
            pybackupper.logger.info("Backup completed successfully.")
            pybackupper.telegram_handler.send_backup_info(pybackupper.config["HOSTNAME"], pybackupper.backups_manager.get_backup_info())
        else:
            pybackupper.logger.error("Backup failed.")
            pybackupper.telegram_handler.send_message(pybackupper.config["HOSTNAME"] + ": Backup failed.")
    except Exception as e:
        pybackupper.telegram_handler.send_message(pybackupper.config["HOSTNAME"] + ": Error occured while creating a backup. " + str(e))
        pybackupper.logger.exception("Error occured while creating a backup.")
