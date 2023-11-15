"""BackupManager class"""

import logging
import logging.config
from singleton import Singleton
from os.path import exists, normpath, getsize, join
from os import makedirs, walk, listdir
from datetime import datetime
from psutil import disk_usage
from json import dump, load
from pprint import pformat
from shutil import Error as shutilError
from concurrent.futures import ThreadPoolExecutor
from backup import Backup
from s3_handler import S3Handler
from telegram_handler import TelegramHandler
from tools import size_to_human_readable, timestamp_to_file_name

class BackupManager(metaclass=Singleton):
    """BackupManager class"""
    def __init__(self,
                src_path:str,
                dest_path:str,
                raw_to_keep:int,
                compressed_to_keep:int,
                s3_to_keep:int,
                ignored:str=None,
                s3_handler=None,
                telegram_handler=None,
                logger:logging.Logger=None) -> None:
        self.logger = logger
        self.src_path = src_path
        self.dest_path = dest_path
        self.ignored = ignored
        self.raw_to_keep = raw_to_keep
        self.compressed_to_keep = compressed_to_keep
        self.s3_to_keep = s3_to_keep
        self.s3_handler = s3_handler
        self.telegram_handler = telegram_handler
        self.backups = {
            "local": [],
            "s3": [],
        }
        self.logger.info("BackupManager initialized.")

    def __str__(self) -> str:
        """Get the string representation of the object.

        Returns:
            str: String representation of the object.
        """
        local_size = 0
        s3_size = 0

        for backup in self.backups["local"]:
            local_size += backup.get_size()

        s3_size = self.s3_handler.get_bucket_size() if not self.s3_handler is None else 0

        return  f"BackupManager:\n" \
                f"  src_path: {self.src_path}\n" \
                f"  dest_path: {self.dest_path}\n" \
                f"  ignored: {self.ignored}\n" \
                f"  raw_to_keep: {self.raw_to_keep}\n" \
                f"  compressed_to_keep: {self.compressed_to_keep}\n" \
                f"  s3_handler: {True if not self.s3_handler is None else False}\n" \
                f"  telegram_handler: {True if not self.telegram_handler is None else False}\n" \
                f"  backups:\n" \
                f"    local:\n" \
                f"      count: {len(self.backups['local'])}\n" \
                f"      size: {size_to_human_readable(local_size)}\n" \
                f"    s3:\n" \
                f"      count: {len(self.backups['s3'])}\n" \
                f"      size: {size_to_human_readable(s3_size)}\n" \
                f"    last:\n" \
                f"      {self.backups['local'][-1].name if len(self.backups['local']) > 0 else None}"

    def __dict__(self) -> dict:
        """Get the dictionary representation of the object.

        Returns:
            dict: Dictionary representation of the object.
        """
        return {
            "src_path": self.src_path,
            "dest_path": self.dest_path,
            "ignored": self.ignored,
            "raw_to_keep": self.raw_to_keep,
            "compressed_to_keep": self.compressed_to_keep,
            "local_size": size_to_human_readable(sum([backup.get_size() for backup in self.backups["local"]])),
            "s3_size": size_to_human_readable(self.s3_handler.get_bucket_size() if not self.s3_handler is None else 0),
            "backups": self.backups,
        }

    @property
    def logger(self) -> logging.Logger:
        """Get the logger.

        Returns:
            logging.Logger: Logger.
        """
        return self._logger

    @logger.setter
    def logger(self, logger:logging.Logger) -> None:
        """Set the logger.

        Args:
            logger (logging.Logger): Logger.
        """
        if logger is None:
            logging.config.fileConfig("log_dev.conf")
            self._logger = logging.getLogger('pybackupper_logger')
        else:
            self._logger = logger

    @property
    def src_path(self) -> str:
        """Get the source path.

        Returns:
            str: Source path.
        """
        return self._src_path

    @src_path.setter
    def src_path(self, src_path:str) -> None:
        """

        Args:
            src_path (str): Source path.

        Raises:
            ValueError: Source path cannot be None or empty.
            FileNotFoundError: Source path does not exist.
        """
        if src_path is None or src_path == "":
            self.logger.error("src_path cannot be None or empty.")
            raise ValueError("src_path cannot be None or empty.")

        src_path = normpath(src_path)
        if not exists(src_path):
            self.logger.error(f"src_path {src_path} does not exist.")
            raise FileNotFoundError(f"src_path {src_path} does not exist.")

        self._src_path = src_path

    @property
    def dest_path(self) -> str:
        """Get the destination path.

        Returns:
            str: Destination path.
        """
        return self._dest_path

    @dest_path.setter
    def dest_path(self, dest_path:str) -> None:
        """

        Args:
            dest_path (str): Destination path.

        Raises:
            ValueError: Destination path cannot be None or empty.
            OSError: Destination path cannot be created.
        """
        if dest_path is None or dest_path == "":
            self.logger.error("dest_path cannot be None or empty.")
            raise ValueError("dest_path cannot be None or empty.")

        dest_path = normpath(dest_path)
        if not exists(dest_path):
            self.logger.warning(f"dest_path {dest_path} does not exist. Creating it.")
            try:
                makedirs(dest_path)
            except OSError as e:
                self.logger.error(f"dest_path {dest_path} cannot be created. {e}")
                raise OSError(f"dest_path {dest_path} cannot be created. {e}")

        self._dest_path = dest_path

    @property
    def ignored(self) -> str:
        """Get the ignored paths.

        Returns:
            str: Ignored paths.
        """
        return self._ignored

    @ignored.setter
    def ignored(self, ignored:str) -> None:
        """

        Args:
            ignored (str): Ignored paths.
        Raises:
            TypeError: ignored must be a string or None.
        """
        if type(ignored) is str or ignored is None:
            self._ignored = ignored
        else:
            self.logger.error("ignored must be a string or None.")
            raise TypeError("ignored must be a string or None.")

    @property
    def raw_to_keep(self) -> int:
        """Get the number of raw backups to keep.

        Returns:
            int: Number of raw backups to keep.
        """
        return self._raw_to_keep

    @raw_to_keep.setter
    def raw_to_keep(self, raw_to_keep:int) -> None:
        """

        Args:
            raw_to_keep (int): Number of raw backups to keep.

        Raises:
            TypeError: raw_to_keep must be an integer.
            ValueError: raw_to_keep must be greater or equal to 0.
        """
        if not type(raw_to_keep) is int:
            self.logger.error("raw_to_keep must be an integer.")
            raise TypeError("raw_to_keep must be an integer.")

        if raw_to_keep < 0:
            self.logger.error("raw_to_keep must be greater or equal to 0.")
            raise ValueError("raw_to_keep must be greater or equal to 0.")

        self._raw_to_keep = raw_to_keep

    @property
    def compressed_to_keep(self) -> int:
        """Get the number of compressed backups to keep.

        Returns:
            int: Number of compressed backups to keep.
        """
        return self._compressed_to_keep

    @compressed_to_keep.setter
    def compressed_to_keep(self, compressed_to_keep:int) -> None:
        """

        Args:
            compressed_to_keep (int): Number of compressed backups to keep.

        Raises:
            TypeError: compressed_to_keep must be an integer.
            ValueError: compressed_to_keep must be greater or equal to 0.
        """
        if not type(compressed_to_keep) is int:
            self.logger.error("compressed_to_keep must be an integer.")
            raise TypeError("compressed_to_keep must be an integer.")

        if compressed_to_keep < 0:
            self.logger.error("compressed_to_keep must be greater or equal to 0.")
            raise ValueError("compressed_to_keep must be greater or equal to 0.")

        self._compressed_to_keep = compressed_to_keep

    @property
    def s3_to_keep(self) -> int:
        """Get the number of S3 backups to keep.

        Returns:
            int: Number of S3 backups to keep.
        """
        return self._s3_to_keep

    @s3_to_keep.setter
    def s3_to_keep(self, s3_to_keep:int) -> None:
        """

        Args:
            s3_to_keep (int): Number of S3 backups to keep.

        Raises:
            TypeError: s3_to_keep must be an integer.
            ValueError: s3_to_keep must be greater or equal to 0.
        """
        if not type(s3_to_keep) is int:
            self.logger.error("s3_to_keep must be an integer.")
            raise TypeError("s3_to_keep must be an integer.")

        if s3_to_keep < 0:
            self.logger.error("s3_to_keep must be greater or equal to 0.")
            raise ValueError("s3_to_keep must be greater or equal to 0.")

        self._s3_to_keep = s3_to_keep

    @property
    def s3_handler(self):
        """Get the S3 handler.

        Returns:
            S3Handler: S3 handler.
        """
        return self._s3_handler

    @s3_handler.setter
    def s3_handler(self, s3_handler) -> None:
        """Set the S3 handler.

        Args:
            s3_handler (S3Handler): S3 handler.

        Raises:
            TypeError: s3_handler must be a S3Handler or None.
        """
        if s3_handler is None:
            self._s3_handler = None
            return

        if not type(s3_handler) is S3Handler:
            self.logger.error("s3_handler must be a S3Handler or None.")
            raise TypeError("s3_handler must be a S3Handler or None.")

        if not s3_handler.test_connection():
            self.logger.error("S3 connection test failed. S3 handler will be set to None.")
            self._s3_handler = None
        else:
            self._s3_handler = s3_handler

    @property
    def telegram_handler(self):
        """Get the Telegram handler.

        Returns:
            TelegramHandler: Telegram handler.
        """
        return self._telegram_handler

    @telegram_handler.setter
    def telegram_handler(self, telegram_handler) -> None:
        """Set the Telegram handler.

        Args:
            telegram_handler (TelegramHandler): Telegram handler.

        Raises:
            TypeError: telegram_handler must be a TelegramHandler or None.
        """
        if telegram_handler is None:
            self._telegram_handler = None
            return

        if not type(telegram_handler) is TelegramHandler:
            self.logger.error("telegram_handler must be a TelegramHandler or None.")
            raise TypeError("telegram_handler must be a TelegramHandler or None.")

        if not telegram_handler.test_connection():
            self.logger.error("Telegram connection test failed. Telegram handler will be set to None.")
            self._telegram_handler = None
        else:
            self._telegram_handler = telegram_handler

    def generate_backup_name(self) -> str:
        """Generate a backup name.

        Returns:
            str: Backup name.
        """
        return timestamp_to_file_name(datetime.now().timestamp())

    def get_src_size(self) -> int:
        """Get the size of the source.

        Returns:
            int: Size of the source.
        """
        self.logger.debug(f"Getting size of {self.src_path}...")
        size = 0

        for root, _, files in walk(self.src_path):
            for file in files:
                size += getsize(join(root, file))

        self.logger.debug(f"Size of {self.src_path} is {size_to_human_readable(size)}.")
        return size
    
    def get_dest_space(self):
        """Get the size of the destination.

        Returns:
            int: Size of the destination.
        """
        self.logger.debug(f"Getting available space of {self.dest_path}...")

        try:
            disk = disk_usage(self.dest_path)
            total = disk.total
            space = disk.free

            if total == 0:
                self.logger.error(f"Error getting available space of {self.dest_path}.")
                return 0

            if space < total * 0.1:
                self.logger.warning(f"Available space of {self.dest_path} is less than 10% of total space.")
                if self.telegram_handler is not None:
                    self.telegram_handler.send_message(
                        f"Available space of {self.dest_path} is less than 10% of total space.\n"
                        f"Space: {size_to_human_readable(space)} / {size_to_human_readable(total)}")
        except OSError:
            space = 0

        self.logger.debug(f"Available space of {self.dest_path} is {size_to_human_readable(space)}.")
        return space

    def check_available_space(self) -> bool:
        """Check if there is enough space to create a backup.

        Returns:
            bool: True if there is enough space, False otherwise.
        """
        src_size = self.get_src_size()
        dest_space = self.get_dest_space()

        if src_size > dest_space:
            self.logger.error(
                f"Not enough space to create a backup."
                f" Source size is {size_to_human_readable(src_size)} " 
                f"and destination space is {size_to_human_readable(dest_space)}.")
            return False

        if len(self.backups["local"]) > 0:
            try:
                compression_ratio = self.backups["local"][-1].calculate_compression_ratio()
            except FileNotFoundError:
                compression_ratio = 1

            return True if ((src_size + (src_size / compression_ratio)) * 1.05) < dest_space else False

        elif self.compressed_to_keep > 0:
            return True if (src_size * 2 * 1.05) < dest_space else False

        return True if (src_size * 1.05) < dest_space else False

    def save_backup_info(self, dest_path:str=None) -> None:
        """Save the backup info to a file.

        Args:
            dest_path (str): Path to save the backup info.

        Raises:
            OSError: Path cannot be created.
        """
        if dest_path is None or dest_path == "":
            dest_path = self.dest_path

        self.logger.debug(f"Saving backup info to {dest_path}...")
        dest_path = normpath(dest_path)

        if not exists(dest_path):
            self.logger.warning(f"Path {dest_path} does not exist. Creating it.")
            try:
                makedirs(dest_path)
            except OSError as e:
                self.logger.error(f"Path {dest_path} cannot be created. {e}")
                raise OSError(f"Path {dest_path} cannot be created. {e}")

        path = normpath(join(dest_path, "backup_info.json"))
        with open(path, "w") as file:
            dump(self.__dict__(), file, indent=4)
        self.logger.debug(f"Backup info saved to {dest_path}.")

    def load_backup_info(self, src_path:str=None, ignore_hash_mismatch:bool=True) -> None:
        """Load the backup info from a file.

        Args:
            src_path (str): Path to load the backup info.
            ignore_hash_mismatch (bool): Ignore hash mismatch and load backup anyway.

        Raises:
            FileNotFoundError: File does not exist.
        """
        if src_path is None or src_path == "":
            src_path = self.dest_path

        self.logger.debug(f"Loading backup info from {src_path}...")
        src_path = normpath(src_path)
        path = normpath(join(src_path, "backup_info.json"))

        if not exists(path):
            self.logger.error(f"File {path} does not exist.")
            raise FileNotFoundError(f"File {path} does not exist.")

        with open(path, "r") as file:
            backup_info = load(file)

        for backup in backup_info["backups"]["local"]:
            tmp_backup = Backup(backup["name"], self.dest_path, self.ignored, self.logger)
            if (backup["raw_hash"] != tmp_backup.calculate_raw_hash(method="sha256")) or \
                (backup["compressed_hash"] != tmp_backup.calculate_compressed_hash(method="sha256")):
                if ignore_hash_mismatch:
                    self.logger.warning(f"Hash mismatch for backup {backup['name']}. Loading backup anyway.")
                else:
                    self.logger.error(f"Hash mismatch for backup {backup['name']}. Skipping backup.")
                    continue

            self.backups["local"].append(tmp_backup)

        self.logger.debug(f"Backup info loaded from {src_path}.")     

    def restore_backup_info(self, src_path:str=None) -> None:
        """Restore the backup info file.

        Args:
            src_path (str): Path to restore the backup info.
        """
        if src_path is None or src_path == "":
            src_path = self.dest_path

        self.logger.debug(f"Restoring backup info from {src_path}...")
        src_path = normpath(src_path)

        backups_list = set()
        for item in listdir(src_path):
            if item.endswith(".zip"):
                backups_list.add(item.split(".")[0])
            else:
                backups_list.add(item)
        
        with ThreadPoolExecutor(len(backups_list)) as executor:
            for backup in backups_list:
                executor.submit(lambda: 
                    self.backups["local"].append(
                        Backup( backup, 
                                self.dest_path, 
                                self.ignored, 
                                self.logger)))

        self.backups["local"].sort(key=lambda x: x.name, reverse=False)

        if len(self.backups["local"]) > 0:
            self.logger.debug(f"Restored backups {pformat(backups_list)} from {src_path}")
            self.save_backup_info()
            if self.telegram_handler is not None:
                self.telegram_handler.send_message(f"Restored backups {pformat(backups_list)} from {src_path}")
            return

        self.logger.warning(f"No backups found in {src_path}.")       
        if self.telegram_handler is not None:
            self.telegram_handler.send_message(f"No backups found in {src_path}.")

    def create_backup(self) -> bool:
        name = self.generate_backup_name()
        self.logger.info(f"Creating backup {name}...")

        if not self.check_available_space():
            self.logger.error("Not enough space to create a backup.")
            return False

        backup = Backup(name, self.dest_path, self.ignored, self.logger)

        try:
            backup.create_raw_backup(self.src_path)
        except FileExistsError:
            self.logger.error(f"Backup {name} already exists.")
            return False
        except FileNotFoundError:
            self.logger.error(f"Source path {self.src_path} does not exist.")
            return False
        except shutilError as e:
            self.logger.error(f"Error creating backup {name}. {e}")
            return False
        try:
            raw_hash = backup.calculate_raw_hash(method="sha256")
        except FileNotFoundError:
            self.logger.error(f"Error calculating raw hash of {backup.name}.")
            return False

        self.logger.debug(f"Raw hash of {backup.name} is {raw_hash}.")

        if self.compressed_to_keep > 0:
            try:
                backup.compress_raw_backup()
                compressed_hash = backup.calculate_compressed_hash(method="sha256")
            except FileNotFoundError:
                self.logger.error(f"Error compressing backup {backup.name}.")
                return False

            self.logger.debug(f"Compressed hash of {backup.name} is {compressed_hash}.")
            self.logger.debug(f"Compression ratio of {backup.name} is {backup.calculate_compression_ratio():.2f}.")

        self.backups["local"].append(backup)
        self.logger.info(f"Backup {backup.name} created.")
        self.logger.debug(f"Backup {backup.name} size is {size_to_human_readable(backup.get_size())}.")
        return True

backup_manager = BackupManager( src_path="../test-source",
                                dest_path="../test-target",
                                raw_to_keep=5,
                                compressed_to_keep=5,
                                ignored=None,
                                s3_handler=None,
                                telegram_handler=None)



try:
    backup_manager.load_backup_info()
except FileNotFoundError:
    print("No backup info found. Restoring it.")
    backup_manager.restore_backup_info()
backup_manager.create_backup()
backup_manager.save_backup_info()
print(backup_manager)