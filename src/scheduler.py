import logging
import logging.config
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from re import fullmatch
from datetime import datetime
from backup_manager import BackupManager
from tools import *
from threading import Thread
from time import sleep

class Scheduler(BlockingScheduler):
    def __init__(self, backupper:BackupManager, trigger:CronTrigger, logger:logging.Logger=None):
        super().__init__()
        self.logger = logger
        self.backupper = backupper
        self.trigger = trigger
        self.sched_job = self.add_job(
            self.backupper.run_backup,
            trigger=self.trigger,
            id="backup_job_" + "_".join(str(x) for x in self.trigger.fields),
            name=f"Backup job with cron: {self.trigger.fields} and timezone: {self.trigger.timezone}")
        self.logger.info(f"Scheduler configured with cron: {self.trigger.fields} and timezone: {self.trigger.timezone}")

    def __del__(self)->None:
        self.shutdown()
        self.logger.info("Scheduler stopped")

    def __str__(self)->str:
        return f"Scheduler with cron: {self.trigger.fields} \
            and timezone: {self.trigger.timezone}. \
            Next run: {timestamp_to_human_readable(
                self.trigger.get_next_fire_time(
                    datetime.now(),
                    datetime.now()).timestamp())}"

    def __dict__(self)->dict:
        return {
            "id": self.sched_job.id,
            "cron": ", ".join(f"{x.name}: {str(x)}" for x in self.trigger.fields),
            "timezone": self.trigger.timezone,
            "next_run": timestamp_to_human_readable(self.trigger.get_next_fire_time(datetime.now(), datetime.now()).timestamp())
        }

    @property
    def logger(self)->logging.Logger:
        """The logger property.

        Returns:
            logging.Logger: The logger instance
        """
        return self._logger

    @logger.setter
    def logger(self, logger:logging.Logger) -> None:
        """Set the logger.

        Args:
            logger (logging.Logger): Logger.
        """
        if logger is None:
            logging.config.fileConfig("log_dev.conf")
            self._logger = logging.getLogger('pybackupper_logger')
        else:
            self._logger = logger

    @property
    def backupper(self)->BackupManager:
        """The backupper property.

        Returns:
            BackupManager: The backupper instance
        """
        return self._backupper

    @backupper.setter
    def backupper(self, backupper:BackupManager) -> None:
        """Set the backupper.

        Raises:
            ValueError: If the backupper is invalid.

        Args:
            backupper (BackupManager): Backupper.
        """
        if backupper is None or not isinstance(backupper, BackupManager):
            self.logger.error("Invalid backupper")
            raise ValueError("Invalid backupper")
        self._backupper = backupper

    @staticmethod
    def to_CronTrigger(minute:str="0", hour:str="0", day_of_week:str="*", day_of_month:str="*", month:str="*", timezone:str="UTC")->CronTrigger:
        """Convert the given parameters to a cron expression.

        Args:
            minute (str, optional): Minute. Defaults to "0".
            hour (str, optional): Hour. Defaults to "0".
            day_of_week (str, optional): Day of week. Defaults to "*". For example: "0,1,2,3,4,5,6" or "MON,TUE,WED,THU,FRI,SAT,SUN".
            day_of_month (str, optional): Day of month. Defaults to "*".
            month (str, optional): Month. Defaults to "*".
            timezone (str, optional): Timezone. Defaults to "UTC".

        Raises:
            ValueError: If one of the parameters is invalid.

        Returns:
            CronTrigger: The CronTrigger instance.
        """

        if minute is None or not isinstance(minute, str):
            raise ValueError("Invalid minute")
        elif minute != "*":
            try:
                i_minute = int(minute)
            except ValueError:
                raise ValueError("Invalid minute")
            else:
                if i_minute < 0 or i_minute > 59:
                    raise ValueError("Invalid minute")

        if hour is None or not isinstance(hour, str):
            raise ValueError("Invalid hour")
        elif hour != "*":
            try:
                i_hour = int(hour)
            except ValueError:
                raise ValueError("Invalid hour")
            else:
                if i_hour < 0 or i_hour > 23:
                    raise ValueError("Invalid hour")

        if day_of_week is None or not isinstance(day_of_week, str):
            raise ValueError("Invalid day of week")

        if day_of_month is None or not isinstance(day_of_month, str):
            raise ValueError("Invalid day of month")
        elif day_of_month != "*":
            try:
                i_day_of_month = int(day_of_month)
            except ValueError:
                raise ValueError("Invalid day of month")
            else:
                if i_day_of_month < 0 or i_day_of_month > 31:
                    raise ValueError("Invalid day of month")
    
        if month is None or not isinstance(month, str):
            raise ValueError("Invalid month")
        elif month != "*":
            try:
                i_month = int(month)
            except ValueError:
                raise ValueError("Invalid month")
            else:
                if i_month < 0 or i_month > 12:
                    raise ValueError("Invalid month")

        if timezone is None or not isinstance(timezone, str):
            raise ValueError("Invalid timezone")
        if not fullmatch(r'^[\w\/\-]+$', timezone):
            raise ValueError("Invalid timezone")

        if day_of_week == "*":
            return CronTrigger(
                minute=minute,
                hour=hour,
                day=day_of_month,
                month=month,
                timezone=timezone)

        day_of_week = day_of_week.lower().split(",")

        for i, day in enumerate(day_of_week):
            if day not in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]:
                try:
                    i_day = int(day)
                except ValueError:
                    raise ValueError("Invalid days of week")
                else:
                    if i_day < 0 or i_day > 6:
                        raise ValueError("Invalid days of week")

                day_of_week[i] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][i_day]

        return CronTrigger(
            minute=minute,
            hour=hour,
            day=day_of_month,
            day_of_week=",".join(day_of_week),
            month=month,
            timezone=timezone)
