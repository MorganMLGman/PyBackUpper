from colorama import Fore, Back, Style
import os
import shutil
from pprint import pprint
from apscheduler.schedulers.blocking import BlockingScheduler

config = {
    "RUNS_TO_KEEP": 7,
    "DAYS_TO_RUN": [1, 3, 5, 7], #1: Monday .... 7: Sunday
    "HOUR": 3,
    "MINUTE": 0,
    "IF_COMPRESS": False,
    "IGNORE_PATTERNS": [""],
}

def read_env():
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
            
            if day in range(1, 8) and day not in days_array:
                days_array.append(day)
            else:
                raise ValueError("Allowed day vales are form 1 to 7, values must be unique")
            
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
        pprint("/source directory exists")
    else:
        raise OSError("/source directory does not exists")
    
    if(os.path.exists("/target")):
        pprint("/target directory exists")
    else:
        raise OSError("/target directory does not exists")

def get_source_size() -> int:
    size = 0
    
    for path, dirs, files in os.walk('/source'):
        for file in files:
            file_path = os.path.join(path, file)
            size += os.path.getsize(file_path)
            
    return size
    
def get_target_space() -> int:
    return shutil.disk_usage('/source').free
    
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
        print(Fore.RED)
        print("Not enough space!")
        print("Minimum required space to create backup is 2 sizes of the source")
        print(f"Source size: {format_file_size(source)}, target available space: {format_file_size(target)}")
        print("Please clear some space. Backup will be resumed on the next planned day.")
        print(Style.RESET_ALL)
        return False

def main():
    read_env()    
    pprint(config)
    check_paths()
    pprint(os.listdir("/source"))
    source_size = get_source_size()
    target_space = get_target_space()
    check_required_space(source_size, target_space)
    # sched = BlockingScheduler()
    # sched.add_job(job, "interval", seconds=5)
    # sched.start()

if __name__ == "__main__":
    main()