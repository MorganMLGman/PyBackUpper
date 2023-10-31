import logging
import logging.config
import inspect
from os.path import exists

class Backup():
    def __init__(self, name:str, dest_path:str, logger:logging.Logger=None) -> None:
        self.logger = logger
        self.name = name
        self.dest_path = dest_path
        self.completed = False
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
