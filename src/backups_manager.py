"""Module to manage backups

Raises:
    FileNotFoundError: Exception raised if source_path does not exist
    FileNotFoundError: Exception raised if target_path does not exist

Returns:
    _type_: BackupManager object
"""
import logging
import logging.config
import json
import os
import shutil
from datetime import datetime
from time import perf_counter
import math
from multiprocessing import cpu_count

class BackupManager():
    def __init__(self, 
                 logger=None, 
                 source_path: str = "/source", 
                 target_path: str = "/target", 
                 s3handler=None, 
                 backup_info_file: str = None,
                 is_compression_enabled: bool = True,
                 archive_format: str = "tar.gz",
                 raw_backup_keep: int = 1, 
                 compressed_backup_keep: int = 7, 
                 s3_raw_keep: int = 1, 
                 s3_compressed_keep: int = 3,
                 ignored_extensions: list = None,
                 puid: int = None,
                 pgid: int = None,):
        """BackupManager class constructor

        Args:
            logger (_type_, optional): Logger if available. Defaults to None.
            source_path (str, optional): Custom absolute path from which backups will be created. Defaults to "/source".
            target_path (str, optional): Custom absolute path where backups will be saved. Defaults to "/target".
            s3handler (_type_, optional): Handle for S3Handler object. Defaults to None.
            backup_info_file (str, optional): Custom backup_info.json file path. Defaults to None.
            raw_backup_keep (int, optional): Number of raw backups to keep. Defaults to 1.
            compressed_backup_keep (int, optional): Number of compressed backups to keep. Defaults to 7.
            s3_raw_keep (int, optional): Number of raw backups to keep in S3. Defaults to 1.
            s3_compressed_keep (int, optional): Number of compressed backups to keep in S3. Defaults to 3.

        Raises:
            FileNotFoundError: Exception raised if source_path does not exist
            FileNotFoundError: Exception raised if target_path does not exist
        """
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
        
        self.is_compression_enabled = is_compression_enabled
        self.archive_format = archive_format
        
        if self.is_compression_enabled and self.map_archive_format(self.archive_format) is None:
            self.logger.error(f"Archive format {self.archive_format} not supported")
            raise ValueError(f"Archive format {self.archive_format} not supported")
                     
        self.raw_backup_keep = raw_backup_keep
        self.compressed_backup_keep = compressed_backup_keep
        self.s3_raw_keep = s3_raw_keep
        self.s3_compressed_keep = s3_compressed_keep
        self.ignored_extensions = ignored_extensions
        
        self.s3handler = s3handler
        
        self.backups = self.load_backup_info_from_file(backup_info_file)
        self.verify_backup_info()
    
    def save_backup_info_to_file(self, file_path: str=None, backup_info: dict = None) -> bool:
        """Function to save the backup_info dictionary to a file

        Args:
            file_path (str, optional): Absolute path to save the file. Defaults to None.
            backup_info (dict, optional): Use custom backup_info instead of the generated one. Defaults to None.

        Returns:
            bool: True if the file was saved successfully, False otherwise
        """
        if file_path is None:
            file_path = os.path.join(self.target_path, "backup_info.json")
                    
        if backup_info is None:
            backup_info = self.backups
            
        try:
            with open(file_path, 'w') as file:
                json.dump(backup_info, file, indent=4)
            self.logger.debug(f"Backup info saved in file {file_path}")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True

    def load_backup_info_from_file(self, file_path: str = None) -> dict:
        """Function to load the backup_info dictionary from a file
        Args:
            file_path (str, optional): Custom absolute path to load the file from. Defaults to None.

        Returns:
            dict: backup_info dictionary
        """
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
        """Function to verify that the backup info is correct and fix it if it is not

        Args:
            target_path (str, optional): Custom absolute path were backups are saved. Defaults to None.

        Returns:
            bool: True if the backup info is correct or was fixed, False otherwise
        """
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
                
        if self.is_compression_enabled:
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
        
        if self.s3handler is not None:
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
                    
            if self.is_compression_enabled:
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
        """Function to create a raw backup

        Args:
            backup_name (str, optional): Custom name for backup. Defaults to None.
            backup_path (str, optional): Custom absolute path to save the backup. Defaults to None.
            source_path (str, optional): Custom absolute path for backup source. Defaults to None.

        Returns:
            bool: True if the backup was created, False otherwise
        """
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
            shutil.copytree(source_path, backup_path, symlinks=True, ignore_dangling_symlinks=True, ignore=shutil.ignore_patterns(*self.ignored_extensions))
            self.logger.debug(f"Backup {backup_name} created")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        
        shutil.chown
            
        for root, dirs, files in os.walk(backup_path):
            for file in files:
                try:
                    source_file = os.path.join(root.replace(backup_path, self.source_path), file)
                    source_file_stat = os.stat(source_file)
                    target_file = os.path.join(root, file)
                    self.logger.debug(f"Copying stats from {source_file} to {target_file}")
                    shutil.copymode(source_file, target_file)
                    self.logger.debug(f"Chowning {target_file} to {source_file_stat.st_uid}:{source_file_stat.st_gid}")
                    shutil.chown(target_file, user=source_file_stat.st_uid, group=source_file_stat.st_gid)
                except Exception as e:
                    self.logger.error(e, exc_info=True)
                    return False
            
            for dir in dirs:
                try:
                    source_dir = os.path.join(root.replace(backup_path, self.source_path), dir)
                    source_dir_stat = os.stat(source_dir)
                    target_dir = os.path.join(root, dir)
                    self.logger.debug(f"Copying stats from {source_dir} to {target_dir}")
                    shutil.copymode(source_dir, target_dir)
                    self.logger.debug(f"Chowning {target_dir} to {source_dir_stat.st_uid}:{source_dir_stat.st_gid}")
                    shutil.chown(target_dir, user=source_dir_stat.st_uid, group=source_dir_stat.st_gid)
                except Exception as e:
                    self.logger.error(e, exc_info=True)
                    return False
        
        if backup_name not in self.backups["local_raw"]:           
            self.backups["local_raw"].append(backup_name)
            self.save_backup_info_to_file()
        
        return True
    
    def map_archive_format(self, archive_format: str, reverse:bool= False) -> str:
        """Function to map archive formatsf

        Args:
            archive_format (str): Archive format to map
            reverse (bool, optional): Map key to value accordingly. Defaults to False.

        Returns:
            str: Mapped archive format
        """
        if archive_format == "" or archive_format is None:
            self.logger.error(f"Empty archive format")
            return None
        
        mapping = {
            "tar": "tar",
            "tar.gz": "gztar",
            "tar.bz2": "bztar",
            "tar.xz": "xztar",
            "zip": "zip"
        }
        
        for k, v in mapping.items():
            if reverse:
                if v == archive_format:
                    return k
            if k == archive_format:
                return v
        return None     
    
    def compress_backup(self, backup_path: str = None, archive_format: str = None) -> bool:
        """Function to compress a backup

        Args:
            backup_path (str, optional): Custom absolute path to read the backup from. Defaults to None.
            archive_format (str, optional): Custom arichve format to compress to. Defaults to "tar.gz".

        Returns:
            bool: True if the backup was compressed, False otherwise
        """            
        if backup_path is None:
            if len(self.backups["local_raw"]) == 0:
                self.logger.error(f"No local raw backups found")
                return False
            backup_path = os.path.join(self.target_path, self.backups["local_raw"][-1])
        
        if not os.path.exists(backup_path):
            self.logger.error(f"Backup path {backup_path} does not exist")
            return False
        
        if archive_format is None:
            archive_format = self.archive_format
        if self.map_archive_format(archive_format) is None:
            self.logger.error(f"Archive format {archive_format} not supported")
            return False
        
        archive_path = backup_path + "." + archive_format
        
        if os.path.exists(archive_path):
            logging.warning(f"Archive {archive_path} already exists")
            if os.path.basename(archive_path) not in self.backups["local_compressed"]:
                self.backups["local_compressed"].append(os.path.basename(archive_path))
                self.save_backup_info_to_file()
            return True  
    
        archive_name = os.path.basename(archive_path)    
                                       
        try:
            shutil_archive_format = self.map_archive_format(archive_format)
            self.logger.debug(f"Compressing backup {backup_path} to {archive_path}")
            # shutil.make_archive(backup_path, shutil_archive_format, backup_path)
            
            match archive_format:
                case "tar":
                    command = f"tar -cf {archive_path} {backup_path}"
                case "tar.gz":
                    command = f"""tar --use-compress-program="pigz -9 -N" -cf {archive_path} {backup_path}"""
                case "tar.bz2":
                    command = f"tar -cjf {archive_path} {backup_path}"
                case "tar.xz":
                    command = f"""tar --use-compress-program="pixz -9" -cf {archive_path} {backup_path}"""
                case "zip":
                    command = f"""tar --use-compress-program="pigz -9 -N --zip" -cf {archive_path} {backup_path}"""
                                    
            os.system(command)               
                
            self.logger.debug(f"Backup {backup_path} compressed to {archive_name}")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        
        self.backups["local_compressed"].append(os.path.basename(archive_name))
        self.save_backup_info_to_file()
        
        return True
    
    def send_raw_backup_to_s3(self, backup_path: str = None) -> bool:
        """Function to send a raw backup to S3

        Args:
            backup_path (str, optional): Custom absolute path to read the backup from. Defaults to None.

        Returns:
            bool: True if the backup was sent to S3, False otherwise
        """
        if self.s3handler is None:
            self.logger.error(f"No S3 handler found")
            return False
        
        if self.s3_raw_keep == 0:
            self.logger.error(f"S3 raw backups disabled")
            return True
        
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
        """Function to send an archive to S3

        Args:
            archive_path (str, optional): Custom absolute path to read the backup archive from. Defaults to None.

        Returns:
            bool: True if the archive was sent to S3, False otherwise
        """
        if self.s3handler is None:
            self.logger.error(f"No S3 handler found")
            return False
        
        if self.s3_compressed_keep == 0:
            self.logger.error(f"S3 compressed backups disabled")
            return True
        
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
    
    def delete_raw_backup(self, backup_name: str = None, backup_path: str = None) -> bool:
        """Function to delete a raw backup

        Args:
            backup_name (str, optional): Custom backup name to be deleted. Defaults to None.
            backup_path (str, optional): Custom absolute path were the backups are saved. Defaults to None.

        Returns:
            bool: True if the backup was deleted, False otherwise
        """
        if backup_name is None:
            if len(self.backups["local_raw"]) == 0:
                self.logger.error(f"No local raw backups found")
                return False       
            backup_name = self.backups["local_raw"][0]
            
        if backup_path is None:
            backup_path = os.path.join(self.target_path, backup_name)
            
        if not os.path.exists(backup_path):
            logging.error(f"Backup {backup_path} does not exist")
            return False
        
        try:
            shutil.rmtree(backup_path)
            self.logger.debug(f"Backup {backup_path} deleted")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        
        self.backups["local_raw"].remove(backup_name)
        self.save_backup_info_to_file()
        
        return True
    
    def delete_compressed_backup(self, backup_name: str = None, backup_path: str = None) -> bool:
        """Function to delete a compressed backup

        Args:
            backup_name (str, optional): Custom archive name to be deleted. Defaults to None.
            backup_path (str, optional): Custom absolute path were archives are saved. Defaults to None.

        Returns:
            bool: True if the archive was deleted, False otherwise
        """
        if backup_name is None:
            if len(self.backups["local_compressed"]) == 0:
                self.logger.error(f"No local compressed backups found")
                return False       
            backup_name = self.backups["local_compressed"][0]
            
        if backup_path is None:
            backup_path = os.path.join(self.target_path, backup_name)
            
        if not os.path.exists(backup_path):
            logging.error(f"Backup {backup_path} does not exist")
            return False
        
        try:
            os.remove(backup_path)
            self.logger.debug(f"Backup {backup_path} deleted")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        
        self.backups["local_compressed"].remove(backup_name)
        self.save_backup_info_to_file()
        
        return True
    
    def delete_s3_raw_backup(self, backup_name: str = None) -> bool:
        """Function to delete a raw backup from S3

        Args:
            backup_name (str, optional): Custom backup name to be deleted. Defaults to None.

        Returns:
            bool: True if the backup was deleted, False otherwise
        """
        if backup_name is None:
            if len(self.backups["s3_raw"]) == 0:
                self.logger.error(f"No S3 raw backups found")
                return False       
            backup_name = self.backups["s3_raw"][0]
            
        try:
            self.s3handler.delete_directory(backup_name)
            self.logger.debug(f"S3 backup {backup_name} deleted")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        
        self.backups["s3_raw"].remove(backup_name)
        self.save_backup_info_to_file()
        
        return True
    
    def delete_s3_compressed_backup(self, backup_name: str = None) -> bool:
        """Function to delete a compressed backup from S3

        Args:
            backup_name (str, optional): Custom archive name to be deleted. Defaults to None.

        Returns:
            bool: True if the archive was deleted, False otherwise
        """
        if backup_name is None:
            if len(self.backups["s3_compressed"]) == 0:
                self.logger.error(f"No S3 compressed backups found")
                return False       
            backup_name = self.backups["s3_compressed"][0]
            
        try:
            self.s3handler.delete_file(backup_name)
            self.logger.debug(f"S3 backup {backup_name} deleted")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        
        self.backups["s3_compressed"].remove(backup_name)
        self.save_backup_info_to_file()
        
        return True
    
    def delete_old_backups_from(self, backup_type: str, max_backups: int) -> bool:
        """Function to delete old backups

        Args:
            backup_type (str): Type of backup to delete (raw, compressed, s3_raw, s3_compressed)
            max_backups (int): Maximum number of backups to keep

        Returns:
            bool: True if the old backups were deleted, False otherwise
        """
        if backup_type not in ["local_raw", "local_compressed", "s3_raw", "s3_compressed"]:
            self.logger.error(f"Invalid backup type {backup_type}")
            return False
        
        if max_backups < 0:
            self.logger.error(f"Invalid max backups {max_backups}")
            return False
        
        backups = self.backups[backup_type]
            
        if len(backups) <= max_backups:
            self.logger.debug(f"No old backups to delete")
            return True
        
        backups_to_delete = len(backups) - max_backups
        self.logger.debug(f"Deleting {backups_to_delete} old backups from {backup_type}")
        
        for _ in range(backups_to_delete):
            match backup_type:
                case "local_raw":
                    self.delete_raw_backup()
                case "local_compressed":
                    self.delete_compressed_backup()
                case "s3_raw":
                    self.delete_s3_raw_backup()
                case "s3_compressed":
                    self.delete_s3_compressed_backup()
                
        return True
    
    def delete_old_backups(self) -> bool:
        """Function to delete old backups

        Returns:
            bool: True if the old backups were deleted, False otherwise
        """
        if not self.delete_old_backups_from("local_raw", self.raw_backup_keep):
            return False
        
        if not self.delete_old_backups_from("local_compressed", self.compressed_backup_keep):
            return False
        
        if self.s3handler is not None:        
            if not self.delete_old_backups_from("s3_raw", self.s3_raw_keep):
                return False
            
            if not self.delete_old_backups_from("s3_compressed", self.s3_compressed_keep):
                return False
        
        return True
    
    def convert_to_human_readable(self, size: int) -> str:
        """Function to convert a size in bytes to human readable format

        Args:
            size (int): Size in bytes

        Returns:
            str: Size in human readable format
        """
        if size == 0:
            return "0B"
        
        power = int(math.log(size, 1024))
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        converted_size = round(size / 1024**power, 2)
        
        return f"{converted_size}{units[power]}"
    
    def convert_time_to_human_readable(self, time: float) -> str:
        """Function to convert time in seconds to human readable format

        Args:
            time (float): Time in seconds

        Returns:
            str: Time in human readable format
        """
        if time == 0:
            return "0s"
        
        power = int(math.log(time, 60))
        units = ["s", "m", "h"]
        if power < 0:
            power = 0
        if power > len(units) - 1:
            power = len(units) - 1
        converted_time = round(time / 60**power, 2)
        
        return f"{converted_time}{units[power]}"
    
    def get_backup_dir_size(self, backup_dir: str=None) -> int:
        """Function to get the size of a backup directory

        Args:
            backup_dir (str, optional): Custom backup directory to get the size from. Defaults to None.

        Raises:
            FileNotFoundError: Exception raised if the backup directory does not exist

        Returns:
            int: Size of the backup directory in bytes
        """
        if backup_dir is None:
            backup_dir = self.target_path
            
        if not os.path.exists(backup_dir):
            self.logger.error(f"Backup directory {backup_dir} does not exist")
            raise FileNotFoundError(f"Backup directory {backup_dir} does not exist")        
        
        backup_dir_size = 0
        
        backup_dir_size = shutil.disk_usage(backup_dir).used
        
        # for root, _, files in os.walk(backup_dir):
        #     for file in files:
        #         try:
        #             backup_dir_size += os.path.getsize(os.path.join(root, file))
        #         except FileNotFoundError as e:
        #             self.logger.error(f"File {os.path.join(root, file)} not found")
        #         except Exception as e:
        #             self.logger.error(e, exc_info=True)
        #             raise e
                
        self.logger.debug(f"Backup directory {backup_dir} size: {self.convert_to_human_readable(backup_dir_size)}")
        
        return backup_dir_size
    
    def get_last_backup_size(self) -> dict:
        """Function to get the size of the last backup

        Returns:
            dict: Dictionary with the size of the last backup in raw and compressed format
        """
        backup_size = {}
        backup_size["raw"] = self.convert_to_human_readable(self.get_backup_dir_size(os.path.join(self.target_path, self.backups["local_raw"][-1])))
        
        if self.backups["local_compressed"][-1].startswith(self.backups["local_raw"][-1]):
            backup_size["compressed"] = self.convert_to_human_readable(os.path.getsize(os.path.join(self.target_path, self.backups["local_compressed"][-1])))
        
        return backup_size
    
    def get_backups_size(self) -> dict:
        """Function to get the size of the backup directory and the S3 bucket

        Returns:
            dict: Dictionary with the size of the backup directory and the S3 bucket
        """
        backup_size = {}
        
        backup_size["local"] = self.convert_to_human_readable(self.get_backup_dir_size())
        
        if self.s3handler is not None:
            backup_size["s3"] = self.convert_to_human_readable(self.s3handler.get_bucket_size())
            
        return backup_size
    
    def get_backup_size(self, backup:str) -> int:
        """Function to get the size of a backup

        Args:
            backup (str): Name of the backup

        Returns:
            int: Size of the backup in bytes
        """
        if backup in self.backups["local_raw"]:
            return self.get_backup_dir_size(os.path.join(self.target_path, backup))
        elif backup in self.backups["local_compressed"]:
            return os.path.getsize(os.path.join(self.target_path, backup))
        
        return 0        
    
    def get_backup_dir_free_space(self, backup_dir: str=None) -> int:
        """Function to get the free space in the backup directory

        Args:
            backup_dir (str, optional): Path to the backup directory. Defaults to None.

        Raises:
            FileNotFoundError: Exception raised if the backup directory does not exist

        Returns:
            int: Free space in the backup directory
        """
        if backup_dir is None:
            backup_dir = self.target_path
            
        if not os.path.exists(backup_dir):
            self.logger.error(f"Backup directory {backup_dir} does not exist")
            raise FileNotFoundError(f"Backup directory {backup_dir} does not exist")        
        
        backup_dir_free_space = shutil.disk_usage(backup_dir).free
        
        self.logger.debug(f"Backup directory free space: {self.convert_to_human_readable(backup_dir_free_space)}")
        
        return backup_dir_free_space
    
    def get_source_dir_size(self, source_path:str=None) -> int:
        """Function to get the size of the source directory

        Args:
            source_path (str, optional): Path to the source directory. Defaults to None.

        Raises:
            FileNotFoundError: Exception raised if the source directory does not exist

        Returns:
            int: Size of the source directory in bytes
        """
        if source_path is None:
            source_path = self.source_path
        
        if not os.path.exists(source_path):
            self.logger.error(f"Source directory {source_path} does not exist")
            raise FileNotFoundError(f"Source directory {source_path} does not exist")
        
        source_dir_size = 0
        for root, _, files in os.walk(source_path):
            for file in files:
                source_dir_size += os.path.getsize(os.path.join(root, file))
                
        self.logger.debug(f"Source directory: {source_path} size: {self.convert_to_human_readable(source_dir_size)}")
        
        return source_dir_size
    
    def check_if_enough_free_space(self, with_archive:bool=True) -> bool:
        """Function to check if there is enough free space in the backup directory

        Args:
            with_archive (bool, optional): If True, the size of the archive will be checked. Defaults to True.

        Returns:
            bool: True if there is enough free space, False otherwise
        """
        source_dir_size = self.get_source_dir_size()
        target_dir_free_space = self.get_backup_dir_free_space()
        
        if with_archive:
            return source_dir_size * 2 < target_dir_free_space
        return source_dir_size < target_dir_free_space
    
    def get_backup_info(self) -> dict:
        """Function to get information about the backup

        Returns:
            dict: Dictionary with information about the backup
        """
        backup_info = {}
        backup_info["backup_dir_free_space"] = self.convert_to_human_readable(self.get_backup_dir_free_space())
        backup_info["last_backup"] = self.backups["local_raw"][-1]
        backup_info["last_backup_size"] = self.get_last_backup_size()
        backup_info["backup_size"] = self.get_backups_size()
        
        if self.s3handler is not None:
            backup_info["s3_bucket_size"] = self.convert_to_human_readable(self.s3handler.get_bucket_size())
                
            try:
                if self.s3handler.check_directory_exists(self.backups["s3_raw"][-1]):
                    backup_info["last_s3_backup"] = self.backups["s3_raw"][-1]
            except Exception:
                try:
                    if self.s3handler.check_file_exists(self.backups["s3_compressed"][-1]):
                        backup_info["last_s3_backup"] = self.backups["s3_compressed"][-1]
                except Exception:
                    pass
            finally:
                if backup_info.get("last_s3_backup", None) is not None:
                    backup_info["last_s3_backup_size"] = "ERROR"
                
                    
        return backup_info
           
    def perform_backup(self)-> str:
        """Function to perform a backup
        
        Returns:
            str: If the backup was successful, returns string with statistics. If the backup failed, returns None

        Raises:
            Exception: Exception raised if any of the backup steps fails
        """
        self.logger.info(f"Performing backup. Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        backup_start_time = perf_counter()
        if not self.create_raw_backup():
            raise Exception("Failed to create raw backup")
        
        compress_start_time = perf_counter()
        if self.is_compression_enabled:
            if not self.compress_backup():
                raise Exception("Failed to compress backup")
        compress_end_time = perf_counter()
        
        upload_start_time = perf_counter()
        if self.s3handler is not None:
            if not self.send_raw_backup_to_s3():
                raise Exception("Failed to send raw backup to S3")
            if self.is_compression_enabled:
                if not self.send_archive_to_s3():
                    raise Exception("Failed to send archive to S3")
        upload_end_time = perf_counter()
        
        if not self.delete_old_backups():
            raise Exception("Failed to delete old backups")
        backup_end_time = perf_counter()
        
        response = f"\
Backup performed in {self.convert_time_to_human_readable(backup_end_time - backup_start_time)}, \
raw backup took {self.convert_time_to_human_readable(compress_start_time - backup_start_time)}, \
compression took {self.convert_time_to_human_readable(compress_end_time - compress_start_time)}, \
upload to S3 took {self.convert_time_to_human_readable(upload_end_time - upload_start_time)}, \
deletion took {self.convert_time_to_human_readable(backup_end_time - upload_end_time)}"

        self.logger.info(response)
        
        return response