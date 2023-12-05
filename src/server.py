import logging
import logging.config
from secrets import token_hex
from flask import Flask, render_template, redirect, url_for, session, request
from flask_wtf.csrf import CSRFProtect
from flask.logging import default_handler
from threading import Thread
from backup_manager import BackupManager
from s3_handler import S3Handler
from scheduler import Scheduler
from tools import *

class Message():
    def __init__(self, message: str, level: str):
        self.message = message if isinstance(message, str) else str(message)
        self.level = level if level in ["primary",
                                        "secondary",
                                        "success",
                                        "danger",
                                        "warning",
                                        "info",
                                        "light",
                                        "dark"] else "info"

    def __str__(self):
        return f"{self.level}: {self.message}"

    def __repr__(self):
        return f"{self.level}: {self.message}"

    def __dict__(self):
        return {
            "message": self.message,
            "level": self.level
        }

    def __eq__(self, other):
        if isinstance(other, Message):
            return self.message == other.message and self.level == other.level
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.message, self.level))

class Server(Flask):
    def __init__(self, backupper: BackupManager, logger: logging.Logger=None):
        super().__init__(__name__)
        self.backupper = backupper
        self.logger = logger
        self.pending_backup = False
        self.schedulers_list = []
        
        super().logger.removeHandler(default_handler)
        super().logger.addHandler(x for x in self.logger.handlers)
        self.add_url_rule('/', view_func=self.index)
        self.add_url_rule('/backup_info', view_func=self.backup_info, methods=['GET'])
        self.add_url_rule('/backup_now', view_func=self.backup_now, methods=['POST'])

        self.add_url_rule('/schedulers', view_func=self.schedulers)
        self.add_url_rule('/add_scheduler', view_func=self.add_scheduler, methods=['POST'])
        self.config["SERVER_NAME"] = "127.0.0.1:5000"
        self.config["SECRET_KEY"] = token_hex(16)
        self.csrf = CSRFProtect(self)

    @property
    def logger(self):
        return self._logger

    @logger.setter
    def logger(self, logger: logging.Logger):
        if logger is None:
            logging.config.fileConfig("log_dev.conf")
            self._logger = logging.getLogger('pybackupper_logger')
        else:
            self._logger = logger
    
    def index(self):
        message = session.pop("message", None)
        backups_dict = self.backupper.__dict__()
        local_size = backups_dict["local_size"] if "local_size" in backups_dict else 0
        s3_size = backups_dict["s3_size"] if "s3_size" in backups_dict else 0
        
        backups = []
        for backup in backups_dict["backups"]["local"]:
            backups.append({
                "name": backup["name"],
                "size": backup["size"],
                "raw": True if backup["completed"] else False,
                "zip": True if backup["compressed"] else False,
                "s3": True if backup in backups_dict["backups"]["s3"] else False,
            })
        
        for backup in backups_dict["backups"]["s3"]:
            if backup not in backups_dict["backups"]["local"]:
                backups.append({
                    "name": backup["name"],
                    "size": size_to_human_readable(self.backupper.s3_handler.get_object_size(backup["name"])),
                    "raw": False,
                    "zip": False,
                    "s3": True,
                })
        
        backups.sort(key=lambda x: x["name"], reverse=True)
        
        return render_template('home.html',
                                pending_backup=self.pending_backup,
                                message=message,
                                backups=backups,
                                local_size=local_size,
                                s3_size=s3_size)

    def backup_info(self):
        return self.backupper.__dict__()

    def backup_now(self):
        self.logger.info("Backup requested")
        message = Message("Backup requested", "info")
        session["message"] = message.__dict__()
        
        self.pending_backup = True
        Thread(target=self.backupper.run_backup, args=(self.backup_callback,)).start()
        
        with self.app_context():
            return redirect(url_for('index'))

    def backup_callback(self, success: bool, message: str):
        message = Message(message, "success" if success else "danger")
        self.pending_backup = False
        with self.app_context():
            return redirect(url_for('index'))

    def schedulers(self):
        tmp_schedulers = [scheduler.__dict__() for scheduler in self.schedulers_list]
        return render_template('schedulers.html', schedulers=tmp_schedulers, pending_backup=self.pending_backup)

    def add_scheduler(self):
        form = {}
        for key in request.form:
            form[key] = request.form[key]
        
        cron = Scheduler.to_cron(
            form["minute1"],
            form["hour1"],
            form["dow1"],
            form["day1"],
            form["month1"])
        
        print(cron)

        self.schedulers_list.append(Scheduler(self.backupper, cron=cron))
        return redirect(url_for('schedulers'))

