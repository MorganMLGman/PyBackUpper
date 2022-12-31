# PyBackUpper
**Docker containerized Python application for creating backups.**

 - [Features](https://github.com/MorganMLGman/PyBackUpper#features)
 - [Docker-compose](https://github.com/MorganMLGman/PyBackUpper#docker-compose)
 - [Changelog](https://github.com/MorganMLGman/PyBackUpper#changelog)

PyBackUpper is simple Python application created to make backups of self-hosted server based on Docker, but could be used to create any kind of backups. For time of writing application can create backups at specified time of the day and specified days of week. Application create copy of the source directory at given time and preserve directory owner, group and permissions. It is also possible to compress created backup with to `.tar.gz` archive. Backups will be deleted from disk after given amount of runs, for example if you specify to run backup every second day and keep 7 runs, first backup will be deleted after two weeks. 

## Features
- Creating backups at specified time of the day
- Creating backups at specified days of the week
- Compressing created backups to `.tar` archive. Currently only `.tar.gz` but will be more
- Deleting old backups
- Preserving owner, group and permissions
- Created `.tar` archive can be created with given owner and group
- Skipping files matching given pattern, like `*.log`

## Docker-compose
```
version: "3.9"
services:
  pybackupper:
    image: ghcr.io/morganmlgman/pybackupper:latest # or morganmlg/pybackupper:tagname
    container_name: pybackupper

    restart: always
    mem_reservation: 2g # optional, but recommended
    mem_limit: 4g # optional, but recommended 

    volumes:
      - /etc/localtime:/etc/localtime:ro # needed to run backups with localtime 
      - /path/to/your/stuff:/source:ro # path to your directory with things you want to backup
      - /path/to/your/drive:/target # path to the destination where you want to keep backups
      - /path/to/logs:/logs # path to directory where you want to keep logs

    environment: # OPTIONAL, default no compression, run every two days at 3AM, 7 runs before deleting
      - PUID=1000 # owner of created archive
      - PGID=1000 # group of created archive
      - IF_COMPRESS=true # if compress backup directory after copying data, currently only .tar.gz but will be more
      - RUNS_TO_KEEP=5 # number of runs to keep before deleting the oldest one
      - DAYS_TO_RUN="0,1,2,3,4,5,6" # days to run, 0 is monday, 6 is sunday, must be in format X,Y,Z and raising order
      - HOUR="2" # hour to run, format 24H, range from 0 to 23
      - MINUTE="30" # minute to run, range from 0 to 59
      - IGNORE_PATTERNS="*.log, *.tar" # files with that extensions will be ignored from backup, format "item1, item2, item3"
```

## Changelog
### [v1.0.5](https://github.com/MorganMLGman/PyBackUpper/pkgs/container/pybackupper/61115320?tag=1.0.5)
- Fix for `"` character in docker-compose environment

### [v1.0.2](https://github.com/MorganMLGman/PyBackUpper/pkgs/container/pybackupper/61115320?tag=1.0.2)
- Removed unnecessary requirement
- Upgraded pip version
- Upgraded wheel and setuptools