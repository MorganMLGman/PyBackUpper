import logging
import logging.config
import json
import os
import shutil
from datetime import datetime
from s3_handler import S3Handler

class BackupManager():
    def __init__(self, logger=None, source_path: str = "/source", target_path: str = "/target", s3handler=None, backup_info_file: str = None):
        if logger is None:
            logging.config.fileConfig("log.conf")
            self.logger = logging.getLogger('pybackupper_logger')
        else:
            self.logger = logger
        
        if not os.path.exists(source_path):
            logger.error(f"Source path {source_path} does not exist")
            raise FileNotFoundError(f"Source path {source_path} does not exist")
            
        self.source_path = source_path
                
        if not os.path.exists(target_path):
            logger.error(f"Target path {target_path} does not exist")         
            raise FileNotFoundError(f"Target path {target_path} does not exist")
        
        self.target_path = target_path
        
        self.s3handler = s3handler
        
        self.backups = self.load_backup_info_from_file(backup_info_file)
        self.verify_backup_info()
    
    def save_backup_info_to_file(self, file_path: str=None, backup_info: dict = None) -> bool:
        if file_path is None:
            file_path = os.path.join(self.target_path, "backup_info.json")
                    
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

    def load_backup_info_from_file(self, file_path: str = None) -> dict:
        if file_path is None:
            file_path = os.path.join(self.target_path, "backup_info.json")
            
        try:
            with open(file_path, 'r') as file:
                backup_info = json.load(file)
            self.logger.debug(f"Backup info loaded from file {file_path}")
        except Exception as e:
            self.logger.warning(e)
            self.logger.warning(f"Backup info file {file_path} not found, creating new one")
            default_backup_info = {
                "local_raw": [],
                "local_compressed": [],
                "s3_raw": [],
                "s3_compressed": []            
            }
            self.save_backup_info_to_file(backup_info=default_backup_info)
            return default_backup_info
        return backup_info
    
    def verify_backup_info(self, target_path:str = None) -> bool:            
        if target_path is None:
            target_path = self.target_path

        if not os.path.exists(target_path):
            self.logger.error(f"Target path {target_path} does not exist")
            return False
             
        directories = [f for f in os.listdir(target_path) if os.path.isdir(os.path.join(target_path, f))]
        
        for directory in directories:
            if directory not in self.backups["local_raw"]:
                self.logger.warning(f"Backup {directory} not listed in backup info")
                self.backups["local_raw"].append(directory)
                self.save_backup_info_to_file()
        
        for backup in self.backups["local_raw"]:
            if backup not in directories:
                self.logger.warning(f"Backup {backup} listed in backup info but not found in target path")
                self.backups["local_raw"].remove(backup)
                self.save_backup_info_to_file()
                
        archives = [f for f in os.listdir(target_path) if os.path.isfile(os.path.join(target_path, f))]
        try:
            archives.remove("backup_info.json")
        except ValueError:
            pass
        
        for archive in archives:
            if archive not in self.backups["local_compressed"]:
                self.logger.warning(f"Backup {archive} not listed in backup info")
                self.backups["local_compressed"].append(archive)
                self.save_backup_info_to_file()
        
        for backup in self.backups["local_compressed"]:
            if backup not in archives:
                self.logger.warning(f"Backup {backup} listed in backup info but not found in target path")
                self.backups["local_compressed"].remove(backup)
                self.save_backup_info_to_file()
        
        s3_directories = self.s3handler.list_directories()
        
        for directory in s3_directories:
            if directory not in self.backups["s3_raw"]:
                self.logger.warning(f"Backup {directory} not listed in backup info")
                self.backups["s3_raw"].append(directory)
                self.save_backup_info_to_file()
        
        for backup in self.backups["s3_raw"]:
            if backup not in s3_directories:
                self.logger.warning(f"Backup {backup} listed in backup info but not found in s3")
                self.backups["s3_raw"].remove(backup)
                self.save_backup_info_to_file()
                
        s3_archives = self.s3handler.list_files()
        
        for archive in s3_archives:
            if archive not in self.backups["s3_compressed"]:
                self.logger.warning(f"Backup {archive} not listed in backup info")
                self.backups["s3_compressed"].append(archive)
                self.save_backup_info_to_file()
        
        for backup in self.backups["s3_compressed"]:
            if backup not in s3_archives:
                self.logger.warning(f"Backup {backup} listed in backup info but not found in s3")
                self.backups["s3_compressed"].remove(backup)
                self.save_backup_info_to_file()
                
        return True
        
    
    def create_raw_backup(self, backup_name: str = None, backup_path: str = None, source_path: str = None) -> bool:
        if backup_name is None:
            backup_name = datetime.today().strftime("%Y_%m_%d_%H_%M_%S")
        
        if backup_path is None:
            backup_path = os.path.join(self.target_path, backup_name)
            
        if source_path is None:
            source_path = self.source_path
            
        if not os.path.exists(source_path):
            self.logger.error(f"Source path {source_path} does not exist")
            return False
        
        if os.path.exists(backup_path):
            logging.warning(f"Backup {backup_path} already exists")
            if backup_name not in self.backups["local_raw"]:
                self.backups["local_raw"].append(backup_name)
                self.save_backup_info_to_file()
            return True
        
        try:
            os.system(f"cp -rp {source_path} {backup_path}")
            self.logger.debug(f"Backup {backup_name} created")
        except Exception as e:
            self.logger.error(e)
            return False
        
        self.backups["local_raw"].append(backup_name)
        self.save_backup_info_to_file()
        
        return True
        
    
    def compress_backup(self, backup_path: str = None, archive_format: str = "gztar") -> bool:        
        if backup_path is None:
            if len(self.backups["local_raw"]) == 0:
                self.logger.error(f"No local raw backups found")
                return False
            elif len(self.backups["local_raw"]) == 1:
                backup_path = os.path.join(self.target_path, self.backups["local_raw"][0])
            else:          
                backup_path = os.path.join(self.target_path, self.backups["local_raw"][-2])
        
        if not os.path.exists(backup_path):
            self.logger.error(f"Backup path {backup_path} does not exist")
            return False
        
        if archive_format not in ["tar", "gztar", "bztar", "xztar"]:
            self.logger.error(f"Archive format {archive_format} not supported")
            return False
        
        archive_path = backup_path + "." + archive_format
        
        if os.path.exists(archive_path):
            logging.warning(f"Archive {archive_path} already exists")
            if os.path.basename(archive_path) not in self.backups["local_compressed"]:
                self.backups["local_compressed"].append(os.path.basename(archive_path))
                self.save_backup_info_to_file()
            return True  
                                    
        try:
            # use multiple threads to compress the backup
            shutil.make_archive(backup_path, archive_format, backup_path, logger=self.logger)
            self.logger.debug(f"Backup {backup_path} compressed to {archive_path}")
        except Exception as e:
            self.logger.error(e)
            return False
        
        self.backups["local_compressed"].append(os.path.basename(archive_path))
        self.save_backup_info_to_file()
        
        return True
    
    def send_raw_backup_to_s3(self, backup_path: str = None) -> bool:
        if backup_path is None:
            if len(self.backups["local_raw"]) == 0:
                self.logger.error(f"No local raw backups found")
                return False       
            backup_path = os.path.join(self.target_path, self.backups["local_raw"][-1])
            
        if not os.path.exists(backup_path):
            logging.error(f"Backup {backup_path} does not exist")
            return False
            
        backup_name = os.path.basename(backup_path)
        
        if backup_name in self.backups["s3_raw"]:
            logging.warning(f"Backup {backup_path} already exists in S3")
            return True

        self.s3handler.upload_directory(backup_path, backup_name)
        self.logger.debug(f"Backup {backup_path} sent to S3")
        self.backups["s3_raw"].append(backup_name)
        self.save_backup_info_to_file()
        
        return True
    
    def send_archive_to_s3(self, archive_path: str = None) -> bool:
        if archive_path is None:
            if len(self.backups["local_compressed"]) == 0:
                self.logger.error(f"No local compressed backups found")
                return False       
            archive_path = os.path.join(self.target_path, self.backups["local_compressed"][-1])
            
        if not os.path.exists(archive_path):
            logging.error(f"Archive {archive_path} does not exist")
            return False
            
        archive_name = os.path.basename(archive_path)
        
        if archive_name in self.backups["s3_compressed"]:
            logging.warning(f"Archive {archive_path} already exists in S3")
            return True

        self.s3handler.upload_file(archive_path, archive_name)
        self.logger.debug(f"Archive {archive_path} sent to S3")
        self.backups["s3_compressed"].append(archive_name)
        self.save_backup_info_to_file()
        
        return True
       
       
