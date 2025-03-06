"""Backup class for pybackupper."""

import logging
import logging.config
import inspect
import shutil
from os import walk, remove, cpu_count
from os.path import exists, join, normpath, getsize
from zipfile import ZipFile, ZIP_BZIP2
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5, sha256, sha512, sha1
from tools import size_to_human_readable

class Backup(dict):
    """Backup class for pybackupper."""
    def __init__(self,
                name:str,
                dest_path:str,
                ignored:str = None,
                logger:logging.Logger=None) -> None:
        """Initializes Backup object.

        Args:
            name (str): Backup name.
            dest_path (str): Destination path of the backup.
            ignored (str): Ignored patterns of the backup.
            logger (logging.Logger, optional): Logger for the class. Defaults to None.
        """
        self.logger = logger
        self.name = name
        self.dest_path = dest_path
        self.ignored = ignored
        super().__init__(self.__dict__())

        try:
            size = self.get_raw_size()
            self.completed =  True if size > 0 else False
        except FileNotFoundError:
            self.completed = False

        self.compressed = True if exists(f"{join(self.dest_path, self.name)}.zip") else False
        super().update(self.__dict__())
        self.logger.debug(f"Backup {self.name} initialized.\n{self}")

    def __str__(self) -> str:
        """Returns string representation of the backup.

        Returns:
            str: String representation of the backup.
        """
        size = size_to_human_readable(self.get_size())

        return  f"Backup {self.name}:\n" \
                f"  Destination path: {self.dest_path}\n" \
                f"  Ignored: {self.ignored}\n" \
                f"  Size: {size}\n" \
                f"  Completed: {self.completed}\n" \
                f"  Compressed: {self.compressed}\n"

    def __dict__(self) -> dict:
        """Returns dictionary representation of the backup.

        Returns:
            dict: Dictionary representation of the backup.
        """
        return {
            "name": self.name,
            "size": size_to_human_readable(self.get_size()),
            "ignored": self.ignored,
            "completed": self.completed,
            "compressed": self.compressed,
            "raw_hash": self.calculate_raw_hash(method="sha256") if self.completed else None,
            "compressed_hash": self.calculate_compressed_hash(method="sha256") if self.compressed else None,
        }

    @property
    def logger(self) -> logging.Logger:
        """Returns logger for the class.

        Returns:
            logging.Logger: Logger for the class.
        """
        return self._logger

    @logger.setter
    def logger(self, logger:logging.Logger) -> None:
        """Sets logger for the class.

        Args:
            logger (logging.Logger): Logger for the class.
        """
        if logger is None:
            logging.config.fileConfig("log_dev.conf")
            self._logger = logging.getLogger('pybackupper_logger')
        else:
            self._logger = logger

    @property
    def name(self) -> str:
        """Returns name of the backup.

        Returns:
            str: Name of the backup.
        """
        return self._name

    @name.setter
    def name(self, name:str) -> None:
        """Sets name of the backup.

        Args:
            name (str): Name of the backup.

        Raises:
            ValueError: Name of the backup is not valid.
            PermissionError: Change of `name` property is not allowed for Backup.
        """
        if name is None or name == "":
            self.logger.error(f"Backup {name} is not valid.")
            raise ValueError(f"Backup {name} is not valid.")

        try:
            if self._name != "":
                self.logger.error("Cannot change name of the backup.")
                raise PermissionError("Cannot change name of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting name of the backup to {name}.")
            self._name = name

    @property
    def dest_path(self) -> str:
        """Returns destination path of the backup.

        Returns:
            str: Destination path of the backup.
        """
        return self._dest_path

    @dest_path.setter
    def dest_path(self, dest_path:str) -> None:
        """Sets destination path of the backup.

        Args:
            dest_path (str): Destination path of the backup.

        Raises:
            ValueError: Destination path of the backup is not valid.
            FileNotFoundError: Destination path of the backup does not exist.
            PermissionError: Change of `dest_path` property is not allowed for Backup.
        """
        if dest_path is None or dest_path == "":
            self.logger.error(f"Backup {dest_path} is not valid.")
            raise ValueError(f"Backup {dest_path} is not valid.")

        if not exists(dest_path):
            self.logger.error(f"Backup {dest_path} does not exist.")
            raise FileNotFoundError(f"Backup {dest_path} does not exist.")

        try:
            if self._dest_path != "":
                self.logger.error("Cannot change destination path of the backup.")
                raise PermissionError("Cannot change destination path of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting destination path of the backup to {dest_path}.")
            self._dest_path = dest_path

        backup_path = join(dest_path, self.name)

        try:
            if exists(backup_path) and self.get_raw_size() > 0:
                self.logger.debug(f"Backup {backup_path} already exists. "\
                    "Marking it as completed.")
                self.completed = True
        except FileNotFoundError:
            pass


    @property
    def completed(self) -> bool:
        """Returns True if backup is completed, False otherwise.

        Returns:
            bool: True if backup is completed, False otherwise.
        """
        try:
            return self._completed
        except AttributeError:
            return False

    @completed.setter
    def completed(self, completed:bool) -> None:
        """Sets completed property of the backup.

        Args:
            completed (bool): True if backup is completed, False otherwise.

        Raises:
            PermissionError: Change of `completed` property is not allowed for Backup.
        """
        caller_class = inspect.currentframe().f_back.f_locals.get("self").__class__.__name__

        if caller_class == self.__class__.__name__:
            self.logger.debug(f"Setting completed property of the backup to {completed}.")
            self._completed = completed
            super().__init__(self.__dict__())
        else:
            self.logger.error(f"Change of `completed` property is not allowed for {caller_class}.")
            raise PermissionError(
                f"Change of `completed` property is not allowed for {caller_class}.")

    @property
    def ignored(self) -> str:
        """Returns ignored patterns of the backup.

        Returns:
            str: Ignored patterns of the backup.
        """
        try:
            return self._ignored
        except AttributeError:
            return "*.sock, *.pid, *.lock"

    @ignored.setter
    def ignored(self, ignored:str) -> None:
        """Sets ignored files of the backup.

        Args:
            ignored (str): Ignored files of the backup.

        Raises:
            ValueError: Ignored files of the backup is not valid.
            PermissionError: Change of `ignored` property is not allowed for Backup.
        """

        if ignored is None or ignored == "":
            ignored = "*.sock, *.pid, *.lock"

        # check if ignored will match pattern "*.ext1, *.ext2, *.ext3, ..."
        if not all([pattern.startswith("*.") for pattern in ignored.split(", ")]):
            self.logger.error(f"Backup {ignored} is not valid.")
            raise ValueError(f"Backup {ignored} is not valid.")

        try:
            if self._ignored != "":
                self.logger.error("Cannot change ignored files of the backup.")
                raise PermissionError("Cannot change ignored files of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting ignored files of the backup to {ignored}.")
            super().__init__(self.__dict__())
            self._ignored = ignored

    @property
    def compressed(self) -> bool:
        """Returns True if backup is compressed, False otherwise.

        Returns:
            bool: True if backup is compressed, False otherwise.
        """
        try:
            return self._compressed
        except AttributeError:
            return False

    @compressed.setter
    def compressed(self, compressed:bool) -> None:
        """Sets compressed property of the backup.

        Args:
            compressed (bool): Compressed property of the backup.

        Raises:
            PermissionError: Change of `compressed` property is not allowed for Backup.
        """
        caller_class = inspect.currentframe().f_back.f_locals.get("self").__class__.__name__

        if caller_class == self.__class__.__name__:
            self.logger.debug(f"Setting compressed property of the backup to {compressed}.")
            self._compressed = compressed
            super().__init__(self.__dict__())
        else:
            self.logger.error(f"Change of `compressed` property is not allowed for {caller_class}.")
            raise PermissionError(
                f"Change of `compressed` property is not allowed for {caller_class}.")


    def get_raw_size(self) -> int:
        """Returns raw size of the backup.

        Returns:
            int: Raw size of the backup.
        """
        backup_path = normpath(join(self.dest_path, self.name))
        self.logger.debug(f"Getting raw size of the backup {self.name}.")

        if not exists(backup_path):
            self.logger.debug(f"Backup {backup_path} does not exist.")
            return 0

        size = sum(getsize(join(root, file))
                for root, _, files in walk(backup_path) for file in files)
        self.logger.debug(f"Raw size of the backup {self.name} is {size_to_human_readable(size)}.")
        return size

    def get_compressed_size(self) -> int:
        """Returns compressed size of the backup.

        Raises:
            FileNotFoundError: Backup does not exist.

        Returns:
            int: Compressed size of the backup.
        """
        backup_path = join(self.dest_path, self.name)
        self.logger.debug(f"Getting compressed size of the backup {self.name}.")

        if not exists(f"{backup_path}.zip"):
            self.logger.error(f"Backup {backup_path}.zip does not exist.")
            raise FileNotFoundError(f"Backup {backup_path}.zip does not exist.")

        size = getsize(f"{backup_path}.zip")
        self.logger.debug(
            f"Compressed size of the backup {self.name} is {size_to_human_readable(size)}.")
        return size

    def get_size(self) -> int:
        """Returns size of the backup.

        Returns:
            int: Size of the backup.
        """
        try:
            raw_size = self.get_raw_size() if self.completed else 0
        except FileNotFoundError:
            raw_size = 0

        try:
            compressed_size = self.get_compressed_size() if self.compressed else 0
        except FileNotFoundError:
            compressed_size = 0

        size = raw_size + compressed_size

        self.logger.debug(
            f"Size of the backup {self.name} is {size}. "\
            f"Human readable: {size_to_human_readable(size)}.")
        return size


    def create_raw_backup(self, src_path:str) -> None:
        """Creates raw backup of the `src_path` to the `dest_path`.

        Args:
            src_path (str): Source path of the backup.

        Raises:
            FileExistsError: Backup is already completed.
            FileNotFoundError: Source path of the backup does not exist.
            shutil.Error: Backup failed.
        """
        if self.completed:
            self.logger.error(f"Backup {self.name} is already completed.")
            raise FileExistsError(f"Backup {self.name} is already completed.")

        if not exists(src_path):
            self.logger.error(f"Backup {src_path} does not exist.")
            raise FileNotFoundError(f"Backup {src_path} does not exist.")

        ignored_extensions = self.ignored.split(", ")

        backup_path = join(self.dest_path, self.name)

        try:
            self.logger.debug(f"Creating raw backup of {src_path} to {self.dest_path}.")
            shutil.copytree(src_path,
                            backup_path,
                            symlinks=True,
                            ignore_dangling_symlinks=True,
                            ignore=shutil.ignore_patterns(*ignored_extensions))
        except shutil.Error as e:
            self.logger.exception(f"Backup {self.name} failed. Exception: {e}.")
            raise e

        self.completed = True
        self.logger.debug(f"Backup {self.name} completed.")

    def _add_to_zip(self, lock: Lock, handle: ZipFile, file_paths_batch: list) -> None:
        """Adds files to the zip file.

        Args:
            lock (Lock): Lock for the zip file.
            handle (ZipFile): Zip file handle.
            file_paths_batch (list): List of file paths to add to the zip file.
        """
        backup_path = normpath(join(self.dest_path, self.name))

        with lock:
            for file_path in file_paths_batch:
                handle.write(file_path,
                            normpath(file_path).replace(backup_path, "").lstrip("\\").lstrip("/"))

    def compress_raw_backup(self) -> None:
        """Compresses raw backup to the zip file.

        Raises:
            FileNotFoundError: Backup is not completed.
            FileNotFoundError: Zip file was not created.
        """

        if not self.completed:
            self.logger.error(f"Backup {self.name} is not completed.")
            raise FileNotFoundError(f"Backup {self.name} is not completed.")

        if self.compressed:
            self.logger.info(f"Backup {self.name} is already compressed. Nothing to do :).")
            return

        self.logger.debug(f"Compressing raw backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        file_paths = []

        for root, _, files in walk(backup_path):
            for file in files:
                file_paths.append(normpath(join(root, file)))

        lock = Lock()

        n_workers = cpu_count() * 2

        self.logger.debug(f"Using {n_workers} workers to compress the backup.")

        chunk_size = len(file_paths) // n_workers
        if chunk_size == 0:
            chunk_size = 1

        with ZipFile(f"{backup_path}.zip", 'w', compression=ZIP_BZIP2) as handle:
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                for i in range(0, len(file_paths), chunk_size):

                    file_paths_batch = file_paths[i:i+chunk_size]

                    _ = executor.submit(self._add_to_zip, lock, handle, file_paths_batch)

        if not exists(f"{backup_path}.zip"):
            self.logger.error(f"Zip file {backup_path}.zip was not created.")
            raise FileNotFoundError(f"Zip file {backup_path}.zip was not created.")

        self.compressed = True
        self.logger.debug(f"Backup {self.name} compressed.")

    def delete_raw_backup(self) -> None:
        """Deletes raw backup.
        """
        self.logger.debug(f"Deleting raw backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        shutil.rmtree(backup_path, ignore_errors=True)

        self.completed = False
        self.logger.debug(f"Backup {self.name} deleted.")

    def delete_compressed_backup(self) -> None:
        """Deletes compressed backup.
        """
        self.logger.debug(f"Deleting compressed backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        try:
            remove(f"{backup_path}.zip")
        except FileNotFoundError:
            pass

        self.compressed = False
        self.logger.debug(f"Backup {self.name} deleted.")

    def delete_backup(self) -> None:
        """Deletes backup.
        """
        self.logger.debug(f"Deleting backup {self.name}.")

        self.delete_raw_backup()
        self.delete_compressed_backup()

        self.completed = False
        self.compressed = False
        self.logger.debug(f"Backup {self.name} deleted.")

    def restore_backup_from_raw(self, restore_path:str) -> None:
        """Restores backup from raw.

        Args:
            restore_path (str): Destination path of the backup.

        Raises:
            FileExistsError: Backup is not completed.
            FileNotFoundError: Backup does not exist.
            shutil.Error: Backup failed.
        """
        backup_path = join(self.dest_path, self.name)

        if not self.completed or not exists(backup_path):
            self.logger.error(f"Backup {self.name} is not completed.")
            raise FileExistsError(f"Backup {self.name} is not completed.")

        try:
            self.logger.debug(f"Restoring backup {self.name} from raw to {restore_path}.")
            shutil.copytree(backup_path,
                            restore_path,
                            symlinks=True,
                            dirs_exist_ok=True,
                            ignore_dangling_symlinks=True)
        except shutil.Error as e:
            self.logger.exception(f"Backup {self.name} failed. Exception: {e}.")
            raise e

        self.completed = True
        self.logger.debug(f"Backup {self.name} completed.")

    def unpack_compressed(self) -> None:
        """Unpacks compressed backup.

        Raises:
            FileNotFoundError: Zip file was not created.
        """
        backup_path = join(self.dest_path, self.name)
        if not self.compressed or not exists(f"{backup_path}.zip"):
            self.logger.error(f"Backup {self.name} is not compressed.")
            raise FileNotFoundError(f"Backup {self.name} is not compressed.")

        self.logger.debug(f"Unpacking compressed backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        with ZipFile(f"{backup_path}.zip", 'r') as handle:
            handle.extractall(backup_path)

        self.completed = True

        self.logger.debug(f"Backup {self.name} unpacked.")

    def calculate_raw_hash(self, method:str) -> str:
        """Calculates hash of the raw backup.

        Returns:
            str: hash of the raw backup.
        Raises:
            FileNotFoundError: Backup is not completed.
            ValueError: Method is not supported.
        """
        methods = {
            "md5": md5,
            "sha1": sha1,
            "sha256": sha256,
            "sha512": sha512
        }

        if method not in methods:
            self.logger.error(f"Method {method} is not supported.")
            raise ValueError(f"Method {method} is not supported. "\
                "Supported methods: md5, sha1, sha256, sha512.")

        backup_path = join(self.dest_path, self.name)

        if not exists(backup_path):
            self.logger.error(f"Backup {self.name} is not completed.")
            raise FileNotFoundError(f"Backup {self.name} is not completed.")

        if not self.completed:
            self.logger.warning(f"Backup {self.name} is not completed. "\
                "Calculating hash of the incomplete backup.")           

        dir_hash = methods[method]()

        for root, _, files in walk(backup_path):
            for file in files:
                with open(join(root, file), "rb") as handle:
                    dir_hash.update(handle.read())

        dir_hash = dir_hash.hexdigest()

        self.logger.debug(f"{method} hash of the raw backup {self.name} is {dir_hash}.")
        return dir_hash

    def calculate_compressed_hash(self, method) -> str:
        """Calculates hash of the compressed backup.

        Returns:
            str: hash of the compressed backup.
        Raises:
            FileNotFoundError: Backup is not completed.
            ValueError: Method is not supported.
        """

        methods = {
            "md5": md5,
            "sha1": sha1,
            "sha256": sha256,
            "sha512": sha512
        }

        if method not in methods:
            self.logger.error(f"Method {method} is not supported.")
            raise ValueError(f"Method {method} is not supported. "\
                "Supported methods: md5, sha1, sha256, sha512.")

        backup_path = join(self.dest_path, self.name)

        if not exists(f"{backup_path}.zip"):
            self.logger.error(f"Backup {self.name} is not completed.")
            raise FileNotFoundError(f"Backup {self.name} is not completed.")

        zip_hash = methods[method]()

        with open(f"{backup_path}.zip", "rb") as handle:
            zip_hash.update(handle.read())

        zip_hash = zip_hash.hexdigest()

        self.logger.debug(f"{method} hash of the compressed backup {self.name} is {zip_hash}.")
        return zip_hash

    def restore_backup(self, restore_path:str) -> bool:
        """Restores backup.

        Args:
            restore_path: Path to restore the backup.

        Raises:
            FileNotFoundError: Restore path does not exist.
            FileNotFoundError: Backup is not available.
            ValueError: Restore path is not valid.

        Returns:
            bool: True if backup was restored successfully, False if errors occurred.
        """
        if restore_path is None or restore_path == "":
            self.logger.error(f"Restore path {restore_path} is not valid.")
            raise ValueError(f"Restore path {restore_path} is not valid.")

        if not exists(restore_path):
            self.logger.error(f"Restore path {restore_path} does not exist.")
            raise FileNotFoundError(f"Restore path {restore_path} does not exist.")

        if self.completed:
            self.logger.info(f"Restoring backup {self.name} from raw to {restore_path}.")
            self.restore_backup_from_raw(restore_path)

        elif self.compressed:
            self.logger.info(f"Restoring backup {self.name} from compressed to {restore_path}.")
            self.unpack_compressed()
            self.restore_backup_from_raw(restore_path)

        else:
            self.logger.error(f"Backup {self.name} is not available.")
            raise FileNotFoundError(f"Backup {self.name} is not available.")

        backup_hash = self.calculate_raw_hash(method="sha256")
        restore_hash = sha256()

        for root, _, files in walk(restore_path):
            for file in files:
                with open(join(root, file), "rb") as handle:
                    restore_hash.update(handle.read())

        restore_hash = restore_hash.hexdigest()

        if backup_hash == restore_hash:
            self.logger.info(f"Backup {self.name} restored to {restore_path} successfully.")
            return True

        self.logger.warning(
            f"Backup {self.name} restored to {restore_path}, but hashes are different. "\
            f"Backup hash: {backup_hash}, restore hash: {restore_hash}.")
        return False

    def calculate_compression_ratio(self) -> float:
        """Calculates compression ratio of the backup.

        Returns:
            float: Compression ratio of the backup.
        """
        try:
            raw_size = self.get_raw_size()
            compressed_size = self.get_compressed_size()

            ratio = raw_size / compressed_size
        except FileNotFoundError:
            ratio = 0.0

        self.logger.debug(f"Compression ratio of the backup {self.name} is {ratio}.")
        return ratio
