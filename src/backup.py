import logging
import logging.config
import inspect
import shutil
from os.path import exists, join

class Backup():
    def __init__(self, name:str, dest_path:str, ignored:str, logger:logging.Logger=None) -> None:
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
        self.completed = False
        self.ignored = ignored
        self.logger.info(f"Backup {self.name} initialized.")
        
    
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
            self.logger.error(f"Backup {name=} is not valid.")
            raise ValueError(f"Backup {name=} is not valid.")
        
        try:
            if self._name != "":
                self.logger.error(f"Cannot change name of the backup.")
                raise PermissionError(f"Cannot change name of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting name of the backup to {name=}.")
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
            self.logger.error(f"Backup {dest_path=} is not valid.")
            raise ValueError(f"Backup {dest_path=} is not valid.")
        
        if not exists(dest_path):
            self.logger.error(f"Backup {dest_path=} does not exist.")
            raise FileNotFoundError(f"Backup {dest_path=} does not exist.")
        
        try:
            if self._dest_path != "":
                self.logger.error(f"Cannot change destination path of the backup.")
                raise PermissionError(f"Cannot change destination path of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting destination path of the backup to {dest_path=}.")
            self._dest_path = dest_path
            
        backup_path = join(self.dest_path, self.name)
        try:
            if exists(f"{backup_path}") and self.get_raw_size() > 0:
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
            self.logger.debug(f"Setting completed property of the backup to {completed=}.")
            self._completed = completed
        else:
            self.logger.error(f"Change of `completed` property is not allowed for {caller_class=}.")
            raise PermissionError(f"Change of `completed` property is not allowed for {caller_class=}.")
        
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
            self.logger.error(f"Backup {ignored=} is not valid.")
            raise ValueError(f"Backup {ignored=} is not valid.")
        
        # check if ignored will match pattern "*.ext1, *.ext2, *.ext3, ..."
        if not all([pattern.startswith("*.") for pattern in ignored.split(", ")]):
            self.logger.error(f"Backup {ignored=} is not valid.")
            raise ValueError(f"Backup {ignored=} is not valid.")
        
        try:
            if self._ignored != "":
                self.logger.error(f"Cannot change ignored files of the backup.")
                raise PermissionError(f"Cannot change ignored files of the backup.")
        except AttributeError:
            self.logger.debug(f"Setting ignored files of the backup to {ignored=}.")
            self._ignored = ignored
            
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
        
        return shutil.disk_usage(backup_path).used


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
            self.logger.error(f"Backup {self.name=} is already completed.")
            raise FileExistsError(f"Backup {self.name=} is already completed.")
        
        if not exists(src_path):
            self.logger.error(f"Backup {src_path=} does not exist.")
            raise FileNotFoundError(f"Backup {src_path=} does not exist.")
        
        ignored_extensions = [x for x in self.ignored.split(", ")] 
        
        try:
            self.logger.debug(f"Creating raw backup of {src_path=} to {self.dest_path=}.")
            shutil.copytree(src_path, 
                            self.dest_path, 
                            symlinks=True, 
                            ignore_dangling_symlinks=True, 
                            ignore=shutil.ignore_patterns(*ignored_extensions))
        except shutil.Error as e:
            self.logger.exception(f"Backup {self.name=} failed. Exception: {e}.")
            raise e
    
        self.completed = True
        self.logger.debug(f"Backup {self.name=} completed.")
        
        
    