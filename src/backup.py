import logging
import logging.config
import inspect
import shutil
from os import walk
from os.path import exists, join, normpath
from tools import size_to_human_readable
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_BZIP2, ZIP_LZMA
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

class Backup():
    def __init__(self, name:str, dest_path:str, ignored:str = None, logger:logging.Logger=None) -> None:
        """Initializes Backup object.

        Args:
            name (str): Backup name.
            dest_path (str): Destination path of the backup.
            ignored (str): Ignored patterns of the backup.
            logger (logging.Logger, optional): Logger for the class. Defaults to None.
        """
        self.logger = logger
        self.completed = False
        self.name = name
        self.dest_path = dest_path
        self.ignored = ignored
        self.compressed = False
        self.logger.info(f"Backup {self.name} initialized.")
        
    def __str__(self) -> str:
        """Returns string representation of the backup.

        Returns:
            str: String representation of the backup.
        """
        
        size = size_to_human_readable(self.get_raw_size())
        
        return  f"Backup {self.name}:\n" \
                f"  Destination path: {self.dest_path}\n" \
                f"  Completed: {self.completed}\n" \
                f"  Ignored: {self.ignored}\n" \
                f"  Size: {size}\n" \
                f"  Compressed: {self.compressed}\n"
        
    
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
                self.logger.error(f"Cannot change name of the backup.")
                raise PermissionError(f"Cannot change name of the backup.")
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
                self.logger.error(f"Cannot change destination path of the backup.")
                raise PermissionError(f"Cannot change destination path of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting destination path of the backup to {dest_path}.")
            self._dest_path = dest_path
        
        backup_path = join(dest_path, self.name)
        
        try:
            if exists(backup_path) and self.get_raw_size() > 0:
                self.logger.warning(f"Backup {backup_path} already exists. Marking it as completed.")
                self.completed = True
        except FileNotFoundError:
            pass
        
        
    @property
    def completed(self) -> bool:
        """Returns True if backup is completed, False otherwise.

        Returns:
            bool: True if backup is completed, False otherwise.
        """
        return self._completed
    
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
        else:
            self.logger.error(f"Change of `completed` property is not allowed for {caller_class}.")
            raise PermissionError(f"Change of `completed` property is not allowed for {caller_class}.")
        
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
            self.logger.error(f"Backup {ignored} is not valid.")
            raise ValueError(f"Backup {ignored} is not valid.")
        
        try:
            if self._ignored != "":
                self.logger.error(f"Cannot change ignored files of the backup.")
                raise PermissionError(f"Cannot change ignored files of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting ignored files of the backup to {ignored}.")
            self._ignored = ignored
            
    @property
    def compressed(self) -> bool:
        """Returns True if backup is compressed, False otherwise.

        Returns:
            bool: True if backup is compressed, False otherwise.
        """
        return self._compressed
    
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
        else:
            self.logger.error(f"Change of `compressed` property is not allowed for {caller_class}.")
            raise PermissionError(f"Change of `compressed` property is not allowed for {caller_class}.")
                
            
    def get_raw_size(self) -> int:
        """Returns raw size of the backup.

        Returns:
            int: Raw size of the backup.
        
        Raises:
            FileNotFoundError: Backup does not exist.
        """
        backup_path = join(self.dest_path, self.name)
        self.logger.debug(f"Getting raw size of the backup {self.name}.")
        
        if not exists(backup_path):
            self.logger.error(f"Backup {backup_path} does not exist.")
            raise FileNotFoundError(f"Backup {backup_path} does not exist.")
        
        size = sum(os.path.getsize(join(root, file)) for root, dirs, files in os.walk(backup_path) for file in files)
        
        self.logger.debug(f"Raw size of the backup {self.name} is {size}. Human readable: {size_to_human_readable(size)}.")
        
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
        
        ignored_extensions = [x for x in self.ignored.split(", ")] 
        
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
            
            file_data = []
            backup_path = join(self.dest_path, self.name)
            for file_path in file_paths_batch:
                with open(join(backup_path, file_path), 'r') as f:
                    file_data.append(f.read())
                    
            with lock:
                for file_path, data in zip(file_paths_batch, file_data):
                    handle.writestr(file_path, data)
                    print(f"Added {file_path} to zip.")
    
    def compress_raw_backup(self) -> None:
        
        if not self.completed:
            self.logger.error(f"Backup {self.name} is not completed.")
            raise FileNotFoundError(f"Backup {self.name} is not completed.")
        
        if self.compressed:
            self.logger.warning(f"Backup {self.name} is already compressed. Nothing to do 😍.")
            return

        self.logger.debug(f"Compressing raw backup {self.name}.")
        backup_path = join(self.dest_path, self.name)
        
        file_paths = []
        
        for root, dirs, files in walk(backup_path):
            for file in files:
                file_paths.append(normpath(join(root, file).lstrip(backup_path)))
        
        lock = Lock()
        
        n_workers = 20        
        chunk_size = len(file_paths) // n_workers
                
        with ZipFile(f"{backup_path}.zip", 'w', compression=ZIP_DEFLATED) as handle:
            with ThreadPoolExecutor(n_workers) as executor:
                for i in range(0, len(file_paths), chunk_size):
                    
                    file_paths_batch = file_paths[i:i+chunk_size]
                                        
                    _ = executor.submit(self._add_to_zip, lock, handle, file_paths_batch)
        
backup = Backup(name="test", dest_path="../test-target")
backup.create_raw_backup(src_path="../test-source")
backup.compress_raw_backup()