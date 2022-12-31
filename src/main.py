import logging
import logging.config
from gc import collect as gc_collect
import os
import shutil
import datetime
from time import perf_counter
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

config = {
    "PUID": 1000,
    "PGID": 1000,
    "RUNS_TO_KEEP": 7,
    "DAYS_TO_RUN": [0, 2, 4, 6], #0: Monday .... 6: Sunday
    "HOUR": 3,
    "MINUTE": 0,
    "IF_COMPRESS": False,
    "IGNORE_PATTERNS": [""],
}

DAY_NAMES = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']

def read_env():
    try:
        config["PUID"] = int(os.environ['PUID'])
    except KeyError:
        config["PUID"] = 1000
    
    if config["PUID"] < 0 or config["PUID"] > 65535:
        logger.exception("Value of PUID must be between 0 and 65535, not %s", config["PUID"])
        raise ValueError("Value of PUID must be between 0 and 65535")
    
    
    try:
        config["PGID"] = int(os.environ['PGID'])
    except KeyError:
        config["PGID"] = 1000
    
    if config["PGID"] < 0 or config["PGID"] > 65535:
        logger.exception("Value of PUID must be between 0 and 65535, not %s", config["PGID"])
        raise ValueError("Value of PGID must be between 0 and 65535")
    
    
    try:
        config["RUNS_TO_KEEP"] = int(os.environ['RUNS_TO_KEEP'])
    except KeyError:
        pass
    except ValueError as e:
        raise ValueError ("RUNS_TO_KEEP must be a number and be greater than 0") from e
    if config["RUNS_TO_KEEP"] == 0:
        raise ValueError ("RUNS_TO_KEEP must be a number and be greater than 0")
    
    try:
        days = os.environ["DAYS_TO_RUN"]
        days = days.split(',')
        days_array = []
        for day in days:
            try:
                day = int(day)
            except ValueError as e:
                raise ValueError("DAYS_TO_RUN must be defined as string with comma as separator, eg. \"1,3,5\" where 1 is Monday, 3 is Wednesday and so on...")
            
            if day in range(0, 7) and day not in days_array:
                days_array.append(day)
            else:
                raise ValueError("Allowed day vales are form 0 to 6, values must be unique")
            
        days_array.sort()
        config["DAYS_TO_RUN"] = days_array  
    except KeyError:
        pass
    
    try:
        config["HOUR"] = int(os.environ['HOUR'])
    except KeyError:
        pass
    except ValueError as e:
        raise ValueError ("HOUR must be a number and be greater or equal to 0 and lesser than 24") from e
    if config["HOUR"] < 0 and config["HOUR"] >= 24:
        raise ValueError ("HOUR must be a number and be greater or equal to 0 and lesser than 24")
    
    try:
        config["MINUTE"] = int(os.environ['MINUTE'])
    except KeyError:
        pass
    except ValueError as e:
        raise ValueError ("MINUTE must be a number and be greater or equal to 0 and lesser than 60")  from e
    if config["MINUTE"] < 0 and config["MINUTE"] >= 60:
        raise ValueError ("MINUTE must be a number and be greater or equal to 0 and lesser than 60")       
    
    try:
        config["IF_COMPRESS"] = True if os.environ['IF_COMPRESS'].lower() in ['true', 'yes', '1'] else False
    except KeyError:
        pass
    except ValueError as e:
        raise ValueError("IF_COMPRESS must be true or false") from e
    
    
    try:
        patterns = os.environ["IGNORE_PATTERNS"]
        patterns = patterns.split(',')
        patterns_array = []
        for pattern in patterns:
            patterns_array.append(pattern.strip())
        config["IGNORE_PATTERNS"] = patterns_array
    except KeyError:
        pass

def check_paths():
    if(os.path.exists("/source")):
        logger.debug("/source directory exists")
    else:
        logger.critical("/source directory does not exists")
        raise OSError("/source directory does not exists")
    
    if(os.path.exists("/target")):
        logger.debug("/target directory exists")
    else:
        logger.critical("/target directory does not exists")
        raise OSError("/target directory does not exists")

def get_source_size() -> int:
    size = 0
    for path, dirs, files in os.walk('/source'):
        for file in files:
            file_path = os.path.join(path, file)
            size += os.path.getsize(file_path)
            
    return size
    
def get_target_space() -> int:
    return shutil.disk_usage('/target').free
    
def format_file_size(size, decimals=2, binary_system=True):
    if binary_system:
        units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB']
        largest_unit = 'YiB'
        step = 1024
    else:
        units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']
        largest_unit = 'YB'
        step = 1000

    for unit in units:
        if size < step:
            return ('%.' + str(decimals) + 'f %s') % (size, unit)
        size /= step

    return ('%.' + str(decimals) + 'f %s') % (size, largest_unit)

def check_required_space(source: int, target: int) -> bool:
    if 2 * source < target:
        return True
    else:
        logger.error("Not enough space!")
        logger.error("Minimum required space to create backup is 2 sizes of the source")
        logger.error("Source size: %s, target available space: %s", format_file_size(source), format_file_size(target))
        logger.error("Please clear some space. Backup will be resumed on the next planned day.")
        return False 
    
def _create_raw_copy_() -> str:
    today = datetime.datetime.today().strftime("%Y_%m_%d_%H_%M")
    backup_dir = os.path.join("/target", today)
    if os.path.exists(backup_dir) and os.path.isdir(backup_dir):
        logger.error("Backup with name \"%s\" already exists. Cannot create new one. Skipping iteration", today)
        return
    else:
        start_time = perf_counter()
        shutil.copytree('/source', backup_dir, ignore=shutil.ignore_patterns(*config["IGNORE_PATTERNS"]))
        end_time = perf_counter()
        logger.info("Raw copy saved to \"%s\". Took %s seconds", backup_dir, round(end_time - start_time, 2))
        return today
    
def _copy_owner_group_(target: str) -> None:
    logger.info("Changing permissions in %s dir according to /source", target)
    
    target_path = os.path.join('/target', target)    
    source_stat = os.stat('/source')
    logger.debug("Source stats: %s", source_stat)
    shutil.chown(target_path, source_stat.st_uid, source_stat.st_gid)
    
    start_time = perf_counter()    
    
    for path, dirs, files in os.walk(target_path):
        if path != target_path:
            src_dir_path = path.replace(target_path, '/source')
            src_dir_path_stat = os.stat(src_dir_path)
            logger.debug("Changing permissions for dir: %s to owner: %s, group %s", path, src_dir_path_stat.st_uid, src_dir_path_stat.st_gid)
            shutil.chown(path, src_dir_path_stat.st_uid, src_dir_path_stat.st_gid)
            
        for file in files:
            dst_file_path = os.path.join(path, file)
            src_file_path = dst_file_path.replace(target_path, '/source')
            src_file_path_stat = os.stat(src_file_path)
            logger.debug("Changing permissions for file: %s to owner: %s, group %s", dst_file_path, src_file_path_stat.st_uid, src_file_path_stat.st_gid)
            shutil.chown(dst_file_path, src_file_path_stat.st_uid, src_file_path_stat.st_gid)
    
    end_time = perf_counter()        
    logger.info("Permission changed. Took %s seconds",  round(end_time - start_time, 2))

def _create_archive_(from_dir: str, archive_list: list, format: str) -> bool:
    backup_dir = os.path.join("/target", from_dir)
    if not os.path.exists(backup_dir):
        logger.error("Cannot create archive from not existing directory. Directory name: \"%s\". Skipping iteration")
        return False
    
    logger.info("Creating %s archive", format)
    start_time = perf_counter()
    archive_name = shutil.make_archive(backup_dir, format, '/target', from_dir, logger=logger)
    shutil.chown(archive_name, config["PUID"], config["PGID"])
    end_time = perf_counter()
        
    archive_name = os.path.basename(archive_name)
    logger.info("Archive \"%s\" created. Took %s seconds", archive_name, round(end_time - start_time, 2))
    archive_list.append(archive_name)    
    return True

    
def run_backup():
    logger.info("Backup started at %s", datetime.datetime.today())
    
    start_time = perf_counter()
    
    check_paths()
    source_size = get_source_size()
    target_space = get_target_space()
    
    if not check_required_space(source_size, target_space):
        return    
    
    old_dirs = [x for x in os.listdir('/target') if os.path.isdir(os.path.join('/target', x))]    
    logger.debug("Available old backup directories: %s", old_dirs)
    
    logger.info("Available space is enough to create new backup. Available space %s", format_file_size(target_space))
    name = _create_raw_copy_()
    
    if name is None:
        return
    
    _copy_owner_group_(name)
        
    if config["IF_COMPRESS"]:
        archive_list = []
        try:
            with open("/target/archive_list.txt", "r") as archive_file:
                for archive_line in archive_file.readlines():
                    archive_list.append(archive_line.strip())
        except FileNotFoundError:
            pass
        
        if not _create_archive_(name, archive_list, "gztar"):
            return
                
        if len(archive_list) > 0:
            while len(archive_list) > config["RUNS_TO_KEEP"]:
                old_archive = archive_list.pop(0)
                logger.debug("Removing old archive: %s", old_archive)
                os.remove(os.path.join('/target', old_archive))
                
            try:
                with open("/target/archive_list.txt", "w") as archive_file:        
                    for archive in archive_list:
                        archive_file.write(f"{archive}\n")
            except Exception as e:
                logger.exception("Error while saving archive_list.txt file", e)
                return
            else:
                for dir in old_dirs:
                    logger.debug("Removing old directory: %s", dir)
                    shutil.rmtree(os.path.join('/target', dir), ignore_errors=True)
                
        else:
            logger.warning("archive_list.txt file was not read. Skipping archive remove part.")
            
    else:
        backup_dirs = [x for x in os.listdir('/target') if os.path.isdir(os.path.join('/target', x))]
        
        backup_list = []
        
        for backup_dir in backup_dirs:
            backup_list.append(datetime.datetime.strptime(backup_dir, "%Y_%m_%d_%H_%M"))
            
        backup_list.sort()
        
        while len(backup_list) > config["RUNS_TO_KEEP"]:
            old_backup = backup_list.pop(0)
            old_backup = old_backup.strftime("%Y_%m_%d_%H_%M")
            logger.debug("Removing old directory: %s", old_backup)
            shutil.rmtree(os.path.join('/target', old_backup))
    
    end_time = perf_counter()
    logger.info("Backup %s created. Took %s", name, round(end_time - start_time, 2))            
    logger.debug("Running garbage collection: %s", gc_collect())

def main():    
    logger.warning("Script is starting")      
    read_env()    
            
    logger.info(config)
    check_paths()
        
    try:
        days_string = ','.join([DAY_NAMES[day] for day in config["DAYS_TO_RUN"]])
        
        sched = BlockingScheduler()
        sched.add_job(run_backup,
                      trigger=CronTrigger(
                          day_of_week=days_string,
                          hour=config["HOUR"],
                          minute=config["MINUTE"]),
                      id='backup',
                      name='Create backup',
                      replace_existing=True)
        sched.start()
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt. Exiting now.")
        exit()

if __name__ == "__main__":
    logging.config.fileConfig("log.conf", disable_existing_loggers=True)
    logger = logging.getLogger('pybackupper_logger')
    main()