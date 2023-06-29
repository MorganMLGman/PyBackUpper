import logging
import logging.config
import json
import os

class BackupManager():
    def __init__(self, logger=None, source_path: str = "/source", target_path: str = "/target", s3handler=None):
        if logger is None:
            logging.config.fileConfig("log.conf")
            self.logger = logging.getLogger('pybackupper_logger')
        else:
            self.logger = logger
            
        self.backups = {
            "local_raw": [],
            "local_compressed": [],
            "s3_raw": [],
            "s3_compressed": []            
        }
        
        self.s3handler = s3handler
        
        if not os.path.exists(source_path):
            logger.error(f"Source path {source_path} does not exist")
            raise FileNotFoundError(f"Source path {source_path} does not exist")
            
        self.source_path = source_path
                
        if not os.path.exists(target_path):
            logger.error(f"Target path {target_path} does not exist")         
            raise FileNotFoundError(f"Target path {target_path} does not exist")
        
        self.target_path = target_path
    
    def save_backup_info_to_file(self, file_path: str, backup_info: dict = None) -> bool:
        if backup_info is None:
            backup_info = self.backups
        try:
            with open(file_path, 'w') as file:
                json.dump(backup_info, file, indent=4)
            self.logger.debug(f"Backup info saved in file {file_path}")
        except Exception as e:
            self.logger.error(e)
            return False
        return True

    def load_backup_info_from_file(self, file_path: str) -> dict:
        try:
            with open(file_path, 'r') as file:
                backup_info = json.load(file)
            self.logger.debug(f"Backup info loaded from file {file_path}")
        except Exception as e:
            self.logger.error(e)
            return None
        return backup_info
    
    def compress_backup(self, backup_path: str = None, archive_format: str = "tar.gz") -> bool:
        if backup_path is None:
            backup_path = os.path.join(self.target_path, self.backups["local_raw"][0])
        
        if not os.path.exists(backup_path):
            self.logger.error(f"Backup path {backup_path} does not exist")
            return False
        
        
        
                
        
        
backup_manager = BackupManager()