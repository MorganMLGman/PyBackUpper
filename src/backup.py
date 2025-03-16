"""Backup class for pybackupper."""

from logger import logger
import shutil
from os import walk, remove, cpu_count
from pathlib import Path
from os.path import exists, join, normpath, getsize
from zipfile import ZipFile, ZIP_BZIP2
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5, sha256, sha512, sha1
from tools import size_to_human_readable

class Backup():
    """Backup class for pybackupper."""
    def __init__(self,
                name:str,
                dest_path:str,
                ignored:str = None) -> None:
        """Initializes Backup object.

        Args:
            name (str): Backup name.
            dest_path (str): Destination path of the backup.
            ignored (str): Ignored patterns of the backup.
            logger (logging.Logger, optional): Logger for the class. Defaults to None.
        """
        
        if name is None or name == "":
            logger.error(f"Backup {name} is not valid.")
            raise ValueError(f"Backup {name} is not valid.")
        self.name = name
        
        self.dest_path = dest_path        
        self.backup_path = Path(self.dest_path).joinpath(self.name).resolve()
        
        self.ignored = ignored

        try:
            size = self.get_raw_size()
            self.completed =  True if size > 0 else False
        except FileNotFoundError:
            self.completed = False

        self.compressed = True if exists(f"{join(self.dest_path, self.name)}.zip") else False
        logger.debug(f"Backup {self.name} initialized.\n{self.to_str()}")

    def to_str(self) -> str:
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

    def to_dict(self) -> dict:
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
            logger.error(f"Backup {dest_path} is not valid.")
            raise ValueError(f"Backup {dest_path} is not valid.")

        if not Path(dest_path).exists():
            logger.error(f"Backup {dest_path} does not exist.")
            raise FileNotFoundError(f"Backup {dest_path} does not exist.")
        
        self._dest_path = Path(dest_path).resolve()

    @property
    def ignored(self) -> str:
        """Returns ignored patterns of the backup.

        Returns:
            str: Ignored patterns of the backup.
        """
        return self._ignored

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
            logger.error(f"Backup {ignored} is not valid.")
            raise ValueError(f"Backup {ignored} is not valid.")
        
        self._ignored = ignored

    def get_raw_size(self) -> int:
        """Returns raw size of the backup.

        Returns:
            int: Raw size of the backup.
        """
        logger.debug(f"Getting raw size of the backup {self.name}.")

        if not self.backup_path.exists():
            logger.debug(f"Backup {self.backup_path} does not exist.")
            return 0

        size = sum(getsize(join(root, file))
                for root, _, files in walk(self.backup_path) for file in files)
        logger.debug(f"Raw size of the backup {self.name} is {size_to_human_readable(size)}.")
        return size

    def get_compressed_size(self) -> int:
        """Returns compressed size of the backup.

        Raises:
            FileNotFoundError: Backup does not exist.

        Returns:
            int: Compressed size of the backup.
        """
        backup_path = join(self.dest_path, self.name)
        logger.debug(f"Getting compressed size of the backup {self.name}.")

        if not exists(f"{backup_path}.zip"):
            logger.error(f"Backup {backup_path}.zip does not exist.")
            raise FileNotFoundError(f"Backup {backup_path}.zip does not exist.")

        size = getsize(f"{backup_path}.zip")
        logger.debug(
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

        logger.debug(
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
            logger.error(f"Backup {self.name} is already completed.")
            raise FileExistsError(f"Backup {self.name} is already completed.")

        if not exists(src_path):
            logger.error(f"Backup {src_path} does not exist.")
            raise FileNotFoundError(f"Backup {src_path} does not exist.")

        ignored_extensions = self.ignored.split(", ")

        backup_path = join(self.dest_path, self.name)

        try:
            logger.debug(f"Creating raw backup of {src_path} to {self.dest_path}.")
            shutil.copytree(src_path,
                            backup_path,
                            symlinks=True,
                            ignore_dangling_symlinks=True,
                            ignore=shutil.ignore_patterns(*ignored_extensions))
        except shutil.Error as e:
            logger.exception(f"Backup {self.name} failed. Exception: {e}.")
            raise e

        self.completed = True
        logger.debug(f"Backup {self.name} completed.")

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
            logger.error(f"Backup {self.name} is not completed.")
            raise FileNotFoundError(f"Backup {self.name} is not completed.")

        if self.compressed:
            logger.info(f"Backup {self.name} is already compressed. Nothing to do :).")
            return

        logger.debug(f"Compressing raw backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        file_paths = []

        for root, _, files in walk(backup_path):
            for file in files:
                file_paths.append(normpath(join(root, file)))

        lock = Lock()

        n_workers = cpu_count() * 2

        logger.debug(f"Using {n_workers} workers to compress the backup.")

        chunk_size = len(file_paths) // n_workers
        if chunk_size == 0:
            chunk_size = 1

        with ZipFile(f"{backup_path}.zip", 'w', compression=ZIP_BZIP2) as handle:
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                for i in range(0, len(file_paths), chunk_size):

                    file_paths_batch = file_paths[i:i+chunk_size]

                    _ = executor.submit(self._add_to_zip, lock, handle, file_paths_batch)

        if not exists(f"{backup_path}.zip"):
            logger.error(f"Zip file {backup_path}.zip was not created.")
            raise FileNotFoundError(f"Zip file {backup_path}.zip was not created.")

        self.compressed = True
        logger.debug(f"Backup {self.name} compressed.")

    def delete_raw_backup(self) -> None:
        """Deletes raw backup.
        """
        logger.debug(f"Deleting raw backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        shutil.rmtree(backup_path, ignore_errors=True)

        self.completed = False
        logger.debug(f"Backup {self.name} deleted.")

    def delete_compressed_backup(self) -> None:
        """Deletes compressed backup.
        """
        logger.debug(f"Deleting compressed backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        try:
            remove(f"{backup_path}.zip")
        except FileNotFoundError:
            pass

        self.compressed = False
        logger.debug(f"Backup {self.name} deleted.")

    def delete_backup(self) -> None:
        """Deletes backup.
        """
        logger.debug(f"Deleting backup {self.name}.")

        self.delete_raw_backup()
        self.delete_compressed_backup()

        self.completed = False
        self.compressed = False
        logger.debug(f"Backup {self.name} deleted.")

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
            logger.error(f"Backup {self.name} is not completed.")
            raise FileExistsError(f"Backup {self.name} is not completed.")

        try:
            logger.debug(f"Restoring backup {self.name} from raw to {restore_path}.")
            shutil.copytree(backup_path,
                            restore_path,
                            symlinks=True,
                            dirs_exist_ok=True,
                            ignore_dangling_symlinks=True)
        except shutil.Error as e:
            logger.exception(f"Backup {self.name} failed. Exception: {e}.")
            raise e

        self.completed = True
        logger.debug(f"Backup {self.name} completed.")

    def unpack_compressed(self) -> None:
        """Unpacks compressed backup.

        Raises:
            FileNotFoundError: Zip file was not created.
        """
        backup_path = join(self.dest_path, self.name)
        if not self.compressed or not exists(f"{backup_path}.zip"):
            logger.error(f"Backup {self.name} is not compressed.")
            raise FileNotFoundError(f"Backup {self.name} is not compressed.")

        logger.debug(f"Unpacking compressed backup {self.name}.")
        backup_path = join(self.dest_path, self.name)

        with ZipFile(f"{backup_path}.zip", 'r') as handle:
            handle.extractall(backup_path)

        self.completed = True

        logger.debug(f"Backup {self.name} unpacked.")

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
            logger.error(f"Method {method} is not supported.")
            raise ValueError(f"Method {method} is not supported. "\
                "Supported methods: md5, sha1, sha256, sha512.")

        backup_path = join(self.dest_path, self.name)

        if not exists(backup_path):
            logger.error(f"Backup {self.name} is not completed.")
            raise FileNotFoundError(f"Backup {self.name} is not completed.")

        if not self.completed:
            logger.warning(f"Backup {self.name} is not completed. "\
                "Calculating hash of the incomplete backup.")           

        dir_hash = methods[method]()

        for root, _, files in walk(backup_path):
            for file in files:
                with open(join(root, file), "rb") as handle:
                    dir_hash.update(handle.read())

        dir_hash = dir_hash.hexdigest()

        logger.debug(f"{method} hash of the raw backup {self.name} is {dir_hash}.")
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
            logger.error(f"Method {method} is not supported.")
            raise ValueError(f"Method {method} is not supported. "\
                "Supported methods: md5, sha1, sha256, sha512.")

        backup_path = join(self.dest_path, self.name)

        if not exists(f"{backup_path}.zip"):
            logger.error(f"Backup {self.name} is not completed.")
            raise FileNotFoundError(f"Backup {self.name} is not completed.")

        zip_hash = methods[method]()

        with open(f"{backup_path}.zip", "rb") as handle:
            zip_hash.update(handle.read())

        zip_hash = zip_hash.hexdigest()

        logger.debug(f"{method} hash of the compressed backup {self.name} is {zip_hash}.")
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
            logger.error(f"Restore path {restore_path} is not valid.")
            raise ValueError(f"Restore path {restore_path} is not valid.")

        if not exists(restore_path):
            logger.error(f"Restore path {restore_path} does not exist.")
            raise FileNotFoundError(f"Restore path {restore_path} does not exist.")

        if self.completed:
            logger.info(f"Restoring backup {self.name} from raw to {restore_path}.")
            self.restore_backup_from_raw(restore_path)

        elif self.compressed:
            logger.info(f"Restoring backup {self.name} from compressed to {restore_path}.")
            self.unpack_compressed()
            self.restore_backup_from_raw(restore_path)

        else:
            logger.error(f"Backup {self.name} is not available.")
            raise FileNotFoundError(f"Backup {self.name} is not available.")

        backup_hash = self.calculate_raw_hash(method="sha256")
        restore_hash = sha256()

        for root, _, files in walk(restore_path):
            for file in files:
                with open(join(root, file), "rb") as handle:
                    restore_hash.update(handle.read())

        restore_hash = restore_hash.hexdigest()

        if backup_hash == restore_hash:
            logger.info(f"Backup {self.name} restored to {restore_path} successfully.")
            return True

        logger.warning(
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

        logger.debug(f"Compression ratio of the backup {self.name} is {ratio}.")
        return ratio
