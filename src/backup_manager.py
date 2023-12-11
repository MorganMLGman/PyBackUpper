"""BackupManager class"""

import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
from singleton import Singleton
from os.path import exists, normpath, getsize, join, isfile, isdir
from os import makedirs, walk, listdir, remove
from datetime import datetime
from psutil import disk_usage
from json import dump, load
from pprint import pformat
from shutil import Error as shutilError
from shutil import rmtree
from concurrent.futures import ThreadPoolExecutor
from re import fullmatch
from botocore.exceptions import ClientError as botocoreClientError
from filecmp import dircmp
from backup import Backup
from s3_handler import S3Handler
from telegram_handler import TelegramHandler
from tools import *

class BackupManager(metaclass=Singleton):
    """BackupManager class"""
    def __init__(self,
                src_path:str,
                dest_path:str,
                raw_to_keep:int,
                compressed_to_keep:int,
                s3_to_keep:int = 0,
                ignored:str=None,
                s3_handler=None,
                telegram_handler=None,
                logger:logging.Logger=None) -> None:
        """Initialize the BackupManager class.

        Args:
            src_path (str): Source path.
            dest_path (str): Destination path.
            raw_to_keep (int): How many raw backups to keep.
            compressed_to_keep (int): How many compressed backups to keep.
            s3_to_keep (int, optional): How many S3 backups to keep. Defaults to 0.
            ignored (str, optional): Ignored paths. Defaults to None.
            s3_handler (_type_, optional): S3 handler. Defaults to None.
            telegram_handler (_type_, optional): Telegram handler. Defaults to None.
            logger (logging.Logger, optional): Logger. Defaults to None.
        """
        self.logger = logger
        self.pending_backup = False
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

        try:
            self.load_backup_info()
        except FileNotFoundError:
            print(listdir(self.dest_path))
            self.logger.warning(f"Backup info not found. Creating it.")
            self.restore_backup_info()
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
                f"  s3_handler: {True if self.s3_handler else False}\n" \
                f"  telegram_handler: {True if self.telegram_handler else False}\n" \
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
            "s3_to_keep": self.s3_to_keep,
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

    @property
    def pending_backup(self) -> bool:
        """Get the pending backup flag.

        Returns:
            bool: Pending backup flag.
        """
        return self._pending_backup

    @pending_backup.setter
    def pending_backup(self, pending_backup:bool) -> None:
        """Set the pending backup flag.

        Args:
            pending_backup (bool): Pending backup flag.
        """
        if not type(pending_backup) is bool:
            self.logger.error("pending_backup must be a boolean.")
            raise TypeError("pending_backup must be a boolean.")

        self._pending_backup = pending_backup

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
                if self.telegram_handler:
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
                if compression_ratio == 0:
                    compression_ratio = 1
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
            botocoreClientError: Error uploading backup info to S3.
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
        self.logger.debug(f"Backup info saved locally to {dest_path}.")

        if self.s3_handler:
            try:
                self.s3_handler.upload_file(path, "backup_info.json")
            except botocoreClientError as e:
                self.logger.exception(f"Error uploading backup info to S3. {e}", exc_info=True)
                raise botocoreClientError(f"Error uploading backup info to S3. {e}")

            self.logger.debug(f"Backup info saved to S3.")

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

        src_path = normpath(src_path)
        path = normpath(join(src_path, "backup_info.json"))
        self.logger.debug(f"Loading backup info from {path}...")

        if not exists(path):
            self.logger.error(f"File {path} does not exist.")
            raise FileNotFoundError(f"File {path} does not exist.")

        with open(path, "r") as file:
            backup_info = load(file)

        for backup in backup_info["backups"]["local"]:
            try:
                tmp_backup = Backup(backup["name"], self.dest_path, self.ignored, self.logger)
            except FileNotFoundError:
                self.logger.error(f"Backup {backup['name']} not found.")
                continue

            if tmp_backup.completed and tmp_backup.compressed:
                if (backup["raw_hash"] != tmp_backup.calculate_raw_hash(method="sha256")) or \
                    (backup["compressed_hash"] != tmp_backup.calculate_compressed_hash(method="sha256")):
                    if ignore_hash_mismatch:
                        self.logger.warning(f"Hash mismatch for backup {backup['name']}. Loading backup anyway.")
                    else:
                        self.logger.error(f"Hash mismatch for backup {backup['name']}. Skipping backup.")
                        continue

            elif tmp_backup.compressed:
                if (backup["compressed_hash"] != tmp_backup.calculate_compressed_hash(method="sha256")):
                    if ignore_hash_mismatch:
                        self.logger.warning(f"Hash mismatch for backup {backup['name']}. Loading backup anyway.")
                    else:
                        self.logger.error(f"Hash mismatch for backup {backup['name']}. Skipping backup.")
                        continue

            self.backups["local"].append(tmp_backup)

        for backup in backup_info["backups"]["s3"]:
            if backup in self.backups["local"]:
                self.backups["s3"].append(backup)
            else:
                try:
                    self.backups["s3"].append(Backup(backup["name"], self.dest_path, self.ignored, self.logger))
                except FileNotFoundError:
                    self.logger.error(f"Backup {backup} not found.")
                    continue

        self.logger.debug(f"Backup info loaded from {src_path}.")     

    def restore_backup_info(self, src_path:str=None) -> None:
        """Restore the backup info file.

        Args:
            src_path (str): Path to restore the backup info.
        """
        pattern = r"\b\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}(?:\.zip)?\b"
        
        if src_path is None or src_path == "":
            src_path = self.dest_path

        self.logger.debug(f"Restoring backup info from {src_path}...")
        src_path = normpath(src_path)

        backups_list = set()
        for item in listdir(src_path):
            if fullmatch(pattern, item):
                if item.endswith(".zip"):
                    backups_list.add(item.split(".")[0])
                else:
                    backups_list.add(item)

        if len(backups_list) > 0:
            with ThreadPoolExecutor(len(backups_list)) as executor:
                for backup in backups_list:
                    executor.submit(lambda: 
                        self.backups["local"].append(
                            Backup( backup, 
                                    self.dest_path, 
                                    self.ignored, 
                                    self.logger)))

        self.backups["local"].sort(key=lambda x: x.name, reverse=False)
        self.backups["s3"].sort(key=lambda x: x.name, reverse=False)
        if len(self.backups["local"]) > 0:
            self.logger.debug(f"Restored backups\n{pformat(backups_list, sort_dicts=False, compact=True, indent=2)}\nfrom {src_path}")
            self.save_backup_info()
            if self.telegram_handler:
                self.telegram_handler.send_message(f"Restored backups\n{pformat(backups_list, sort_dicts=False, compact=True, indent=2)}\nfrom {src_path}")
            return

        self.logger.warning(f"No backups found in {src_path}.")       
        if self.telegram_handler:
            self.telegram_handler.send_message(f"No backups found in {src_path}.")

    # TODO: Add method to verify backup_info.json integrity

    def create_backup(self) -> bool:
        """Create a backup.

        Returns:
            bool: True if the backup has been created, False otherwise.
        """
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

    def upload_backup_to_s3(self, backup_name:str) -> bool:
        """Upload a backup to S3.

        Args:
            backup_name (str): Name of the backup to upload.

        Returns:
            bool: True if the backup has been uploaded, False otherwise.
        """
        if self.s3_handler is None:
            self.logger.error("S3 handler is not set.")
            return False

        self.logger.debug(f"Uploading backup {backup_name} to S3...")
        index = self.get_backup_index_by_name(backup_name)

        if index == -1:
            self.logger.error(f"Backup {backup_name} not found.")
            return False

        if not self.backups["local"][index].completed or \
            not self.backups["local"][index].compressed:
            self.logger.error(f"Backup {backup_name} is not completed or compressed.")
            return False

        try:
            self.s3_handler.upload_file(self.backups["local"][index].dest_path + "/"
                                        + backup_name + ".zip", backup_name + ".zip")
        except FileNotFoundError:
            self.logger.error(f"Backup {backup_name} not found.")
            return False
        except botocoreClientError as e:
            self.logger.exception(f"Error uploading backup {backup_name} to S3. {e}", exc_info=True)
            return False

        self.backups["s3"].append(self.backups["local"][index])
        self.logger.debug(f"Backup {backup_name} uploaded to S3.")
        return True

    def download_backup_from_s3(self, backup_name:str) -> bool:
        """Download a backup from S3.

        Args:
            backup_name (str): Name of the backup to download.

        Returns:
            bool: True if the backup has been downloaded, False otherwise.
        """
        if self.pending_backup:
            self.logger.error("Backup task is running, need to wait for it to finish.")
            return False
        self.pending_backup = True
        if self.s3_handler is None:
            self.logger.error("S3 handler is not set.")
            return False

        self.logger.debug(f"Downloading backup {backup_name} from S3...")

        for backup in self.backups["local"]:
            if backup.name == backup_name and backup.compressed:
                self.logger.debug(f"Backup {backup_name} already exists.")
                self.pending_backup = False
                return True

        try:
            self.s3_handler.download_file(backup_name + ".zip",
                                        self.dest_path + "/" +  backup_name + ".zip")
        except botocoreClientError as e:
            self.logger.exception(f"Error downloading backup {backup_name} from S3. {e}", exc_info=True)
            self.pending_backup = False
            return False

        if not exists(self.dest_path + "/" + backup_name + ".zip"):
            self.logger.error(f"Downloaded backup {backup_name} not found.")
            self.pending_backup = False
            return False

        self.logger.debug(f"Backup {backup_name} downloaded from S3.")
        backup = Backup(backup_name, self.dest_path, self.ignored, self.logger)
        self.backups["local"].append(backup)
        self.backups["s3"][self.get_backup_index_by_name(backup_name, from_s3=True)].update(backup)
        self.logger.debug(f"Backup {backup_name} added to local backups.")
        try:
            self.save_backup_info()
        except Exception as e:
            self.logger.exception(f"Error saving backup info. {e}", exc_info=True)
        self.pending_backup = False
        return True

    def get_backup_index_by_name(self, backup_name:str, from_s3:bool=False) -> int:
        """Get the index of a backup by its name.

        Args:
            backup_name (str): Name of the backup.

        Returns:
            int: Index of the backup.
        """
        if from_s3 and self.s3_handler is None:
            self.logger.error("S3 handler is not set.")
            return -1

        for index, backup in enumerate(self.backups["local"] if not from_s3 else self.backups["s3"]):
            if backup.name == backup_name:
                return index
        return -1

    def delete_raw_backup(self, backup_name:str) -> bool:
        """Delete a raw backup.

        Args:
            backup_name (str): Name of the backup to delete.

        Returns:
            bool: True if the backup has been deleted, False otherwise.
        """
        self.logger.debug(f"Deleting raw backup {backup_name}...")
        index = self.get_backup_index_by_name(backup_name)

        if index == -1:
            self.logger.error(f"Backup {backup_name} not found.")
            return False

        try:
            backup = self.backups["local"][index]
            self.backups["local"][index].delete_raw_backup()
            if not self.backups["local"][index].completed and \
                not self.backups["local"][index].compressed:
                _ = self.backups["local"].pop(index)
            if self.s3_handler:
                index_s3 = self.get_backup_index_by_name(backup.name, from_s3=True)
                self.backups["s3"][index_s3].update(backup)
        except FileNotFoundError:
            self.logger.error(f"Backup {backup_name} not found.")
            return False
        except botocoreClientError as e:
            self.logger.exception(f"Error updating backup {backup_name} in S3. {e}", exc_info=True)
            pass

        self.logger.debug(f"Raw backup {backup_name} deleted.")
        return True

    def delete_compressed_backup(self, backup_name:str) -> bool:
        """Delete a compressed backup.

        Args:
            backup_name (str): Name of the backup to delete.

        Returns:
            bool: True if the backup has been deleted, False otherwise.
        """
        self.logger.debug(f"Deleting compressed backup {backup_name}...")
        index = self.get_backup_index_by_name(backup_name)

        if index == -1:
            self.logger.error(f"Backup {backup_name} not found.")
            return False

        try:
            backup = self.backups["local"][index]
            self.backups["local"][index].delete_compressed_backup()
            if not self.backups["local"][index].completed and \
                not self.backups["local"][index].compressed:
                _ = self.backups["local"].pop(index)
            if self.s3_handler:
                index_s3 = self.get_backup_index_by_name(backup.name, from_s3=True)
                self.backups["s3"][index_s3].update(backup)
        except FileNotFoundError:
            self.logger.error(f"Backup {backup_name} not found.")
            return False
        except botocoreClientError as e:
            self.logger.exception(f"Error updating backup {backup_name} in S3. {e}", exc_info=True)
            pass

        self.logger.debug(f"Compressed backup {backup_name} deleted.")
        return True

    def delete_s3_backup(self, backup_name:str) -> bool:
        """Delete a S3 backup.

        Args:
            backup_name (str): Name of the backup to delete.

        Returns:
            bool: True if the backup has been deleted, False otherwise.
        """
        if self.s3_handler is None:
            self.logger.error("S3 handler is not set.")
            return False

        self.logger.debug(f"Deleting S3 backup {backup_name}...")
        index = self.get_backup_index_by_name(backup_name, from_s3=True)

        if index == -1:
            self.logger.error(f"Backup {backup_name} not found.")
            return False

        try:
            self.s3_handler.delete_file(backup_name + ".zip")
            _ = self.backups["s3"].pop(index)
        except botocoreClientError as e:
            self.logger.exception(f"Error deleting backup {backup_name} from S3. {e}", exc_info=True)
            return False

        self.logger.debug(f"S3 backup {backup_name} deleted.")
        return True

    def delete_backup(self, backup_name:str) -> bool:
        """Delete a backup.

        Args:
            backup_name (str): Name of the backup to delete.

        Returns:
            bool: True if the backup has been deleted, False otherwise.
        """
        if self.pending_backup:
            self.logger.error("Backup task is running, need to wait for it to finish.")
            return False
        self.pending_backup = True
        self.logger.debug(f"Deleting backup {backup_name}...")
        index = self.get_backup_index_by_name(backup_name)

        if index == -1:
            self.logger.error(f"Backup {backup_name} not found.")
        else:
            try:
                self.backups["local"][index].delete_backup()
            except FileNotFoundError:
                pass
            _ = self.backups["local"].pop(index)

        if self.s3_handler:
            try:
                self.s3_handler.delete_file(backup_name + ".zip")
                _ = self.backups["s3"].pop(self.get_backup_index_by_name(backup_name, from_s3=True))
            except botocoreClientError as e:
                self.logger.exception(f"Error deleting backup {backup_name} from S3. {e}", exc_info=True)

        self.logger.debug(f"Backup {backup_name} deleted.")
        try:
            self.save_backup_info()
        except Exception as e:
            self.logger.exception(f"Error saving backup info. {e}", exc_info=True)
        self.pending_backup = False
        return True

    def delete_old_backups(self) -> None:
        """Delete old backups."""
        self.logger.debug(f"Deleting old backups...")
        backups_to_delete = {
            "raw": [],
            "compressed": [],
            "s3": [],
        }
        if len(self.backups["local"]) > self.raw_to_keep:
            for backup in self.backups["local"][0:len(self.backups["local"]) - self.raw_to_keep]:
                if backup.completed:
                    backups_to_delete["raw"].append(backup.name)

        if len(self.backups["local"]) > self.compressed_to_keep:
            for backup in self.backups["local"][0:len(self.backups["local"]) - self.compressed_to_keep]:
                if backup.compressed:
                    backups_to_delete["compressed"].append(backup.name)

        if len(self.backups["s3"]) > self.s3_to_keep:
            for backup in self.backups["s3"][0:len(self.backups["s3"]) - self.s3_to_keep]:
                backups_to_delete["s3"].append(backup.name)

        self.logger.info(f"Deleting backups\n{pformat(backups_to_delete, sort_dicts=False, compact=True, indent=2)}\n...")

        for backup in backups_to_delete["raw"]:
            self.delete_raw_backup(backup)
        for backup in backups_to_delete["compressed"]:
            self.delete_compressed_backup(backup)
        for backup in backups_to_delete["s3"]:
            self.delete_s3_backup(backup)

        self.logger.debug(f"Old backups deleted.")

    def clear_dest_path(self) -> None:
        self.logger.debug(f"Clearing {self.dest_path}...")
        for item in listdir(self.dest_path):
            item_path = join(self.dest_path, item)
            if isfile(item_path):
                remove(item_path)
            elif isdir(item_path):
                rmtree(item_path)
        self.logger.debug(f"{self.dest_path} cleared.")

    def unzip_backup(self, backup_name:str) -> bool:
        """Unzip a backup.

        Args:
            backup_name (str): Name of the backup to unzip.

        Returns:
            bool: True if the backup has been unzipped, False otherwise.
        """
        if self.pending_backup:
            self.logger.error("Backup task is running, need to wait for it to finish.")
            return False
        self.pending_backup = True
        self.logger.debug(f"Unzipping backup {backup_name}...")
        index = self.get_backup_index_by_name(backup_name)

        if index == -1:
            self.logger.error(f"Backup {backup_name} not found.")
            return False

        try:
            self.backups["local"][index].unpack_compressed()
        except FileNotFoundError:
            self.logger.error(f"Backup {backup_name} not found.")
            return False

        if not exists(join(self.dest_path, backup_name)):
            self.logger.error(f"Unzipped backup {backup_name} not found.")
            return False

        self.logger.debug(f"Backup {backup_name} unzipped.")

        if self.s3_handler:
            self.backups["s3"][self.get_backup_index_by_name(backup_name, from_s3=True)].update(self.backups["local"][index])
            
        self.pending_backup = False
        return True

    def restore_backup(self, backup_name:str, restore_path:str) -> bool:
        """Restore a backup.

        Args:
            backup_name (str): Name of the backup to restore.
            restore_path (str): Path to restore the backup.

        Returns:
            bool: True if the backup has been restored, False otherwise.
        """
        if self.pending_backup:
            self.logger.error("Backup task is running, need to wait for it to finish.")
            return False
        self.pending_backup = True
        self.logger.debug(f"Restoring backup {backup_name}...")
        index = self.get_backup_index_by_name(backup_name)

        if index == -1:
            self.logger.error(f"Backup {backup_name} not found.")
            return False

        try:
            self.backups["local"][index].restore_backup_from_raw(restore_path)
        except FileNotFoundError as e:
            self.logger.exception(f"Error restoring backup {backup_name}. {e}", exc_info=True)
            return False
        except shutilError as e:
            self.logger.exception(f"Error restoring backup {backup_name}. {e}", exc_info=True)
            return False
        
        if dircmp(self.src_path, join(self.dest_path, backup_name)).diff_files != []:
            self.logger.error(f"Error restoring backup {backup_name}.")
            return False

        self.logger.debug(f"Backup {backup_name} restored.")

        if self.s3_handler:
            self.backups["s3"][self.get_backup_index_by_name(backup_name, from_s3=True)].update(self.backups["local"][index])
            
        self.pending_backup = False
        return True

    def run_backup(self, callback=None) -> str:
        """Run a backup.

        Returns:
            str: Backup name.
        """
        if self.pending_backup:
            self.logger.error("A backup is already running.")
            return None
        self.pending_backup = True
        start_time = datetime.now().timestamp()
        self.logger.info(f"Running backup. Start time: {timestamp_to_human_readable(start_time)}.")

        if self.create_backup():
            end_time = datetime.now().timestamp()

            if self.s3_handler:
                s3_result = self.upload_backup_to_s3(self.backups["local"][-1].name)
                upload_end_time = datetime.now().timestamp()

            if callback:
                callback(True, "Backup completed.")

            self.delete_old_backups()

            self.logger.info(f"Backup completed. End time: {timestamp_to_human_readable(end_time)}.")
            self.logger.info(f"Backup duration: {time_diff_to_human_readable(round(end_time - start_time))}.")
            self.logger.info(f"""Backup info:\n{pformat(self.backups["local"][-1].__dict__(), sort_dicts=False, indent=2, compact=True)}.""")
            if self.s3_handler and s3_result:
                self.logger.info(f"Backup uploaded to S3. Upload duration: {time_diff_to_human_readable(round(upload_end_time - end_time))}.")
            elif self.s3_handler and not s3_result:
                self.logger.error(f"Backup upload to S3 failed.")

            if self.telegram_handler:
                telegram_message = \
                    f"*Backup completed*\\.\n" \
                    f"""Start time: *{timestamp_to_human_readable(start_time).replace("-", "\\-")}*\\.\n""" \
                    f"End time: *{timestamp_to_human_readable(end_time).replace("-", "\\-")}*\\.\n" \
                    f"Backup duration: *{time_diff_to_human_readable(round(end_time - start_time))}*\\.\n" \
                    f"Backup info:\n```json\n{pformat(self.backups['local'][-1].__dict__(), sort_dicts=False, indent=2, compact=True)}```\n"
                
                if self.s3_handler:
                    if s3_result:
                        telegram_message += \
                            f"Backup uploaded to S3: *{True if self.s3_handler and s3_result else False}*\\.\n" \
                            f"Upload duration: *{time_diff_to_human_readable(round(upload_end_time - end_time)) if s3_result else 0}*\\.\n"

                        try:
                            s3_size = self.s3_handler.get_bucket_size()
                            telegram_message += \
                                f"S3 size: *{size_to_human_readable(s3_size).replace(".", "\\.")}*\\.\n"
                        except botocoreClientError:
                            self.logger.error(f"Error getting S3 size.")
                            telegram_message += \
                                f"*Error getting S3 size\\.*\n"

                    else:
                        telegram_message += \
                            f"*Backup uploaded to S3 failed\\.*\n"

                self.telegram_handler.send_message(
                    telegram_message,
                    markdown=True)
        else:
            self.logger.error(f"Backup failed.")

            if self.telegram_handler:
                for index, handler in enumerate(self.logger.handlers):
                    if type(handler) is TimedRotatingFileHandler:
                        break

                path = normpath(self.logger.handlers[index].baseFilename)
                self.telegram_handler.send_file(path, caption=f"Backup failed at {timestamp_to_human_readable(datetime.now().timestamp())}.")

        try:
            self.save_backup_info()
        except OSError:
            self.logger.error(f"Backup info cannot be saved.")
            self.logger.error(f"Printing backup info:\n{pformat(self.__dict__(), sort_dicts=False, compact=True, indent=2)}")

            if self.telegram_handler:
                self.telegram_handler.send_message(
                    f"*Backup info cannot be saved\\.*\n" \
                    f"Printing backup info:\n```json\n{pformat(self.__dict__(), sort_dicts=False, compact=True, indent=2)}```\n",
                    markdown=True)
        except botocoreClientError:
            self.logger.error(f"Backup info cannot be uploaded to S3.")
            self.logger.error(f"Printing backup info:\n{pformat(self.__dict__(), sort_dicts=False, compact=True, indent=2)}")

            if self.telegram_handler:
                self.telegram_handler.send_message(
                    f"*Backup info cannot be uploaded to S3\\.*\n" \
                    f"Printing backup info:\n```json\n{pformat(self.__dict__(), sort_dicts=False, compact=True, indent=2)}```\n",
                    markdown=True)

        self.pending_backup = False
        return self.backups["local"][-1].name

