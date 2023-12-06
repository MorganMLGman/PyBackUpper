import logging
import logging.config
from secrets import token_hex
from flask import (
    Flask, render_template, redirect, url_for, session, request, send_file)
from flask_wtf.csrf import CSRFProtect
from flask.logging import default_handler
from threading import Thread
from tzlocal import get_localzone
from backup_manager import BackupManager
from s3_handler import S3Handler
from scheduler import Scheduler
from tools import *
from time import sleep

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
        self.schedulers_list = []
        
        super().logger.removeHandler(default_handler)
        super().logger.addHandler(x for x in self.logger.handlers)
        self.add_url_rule('/', view_func=self.index)
        self.add_url_rule('/backup_info', view_func=self.backup_info, methods=['GET'])
        self.add_url_rule('/info/<name>', view_func=self.single_backup_info, methods=['GET'])
        self.add_url_rule('/backup_now', view_func=self.backup_now, methods=['POST'])
        self.add_url_rule('/download/<name>', view_func=self.download_backup, methods=['GET'])
        
        self.add_url_rule('/restore_backup', view_func=self.restore_backup, methods=['POST'])
        self.add_url_rule('/unzip_backup', view_func=self.unzip_backup, methods=['POST'])
        self.add_url_rule('/download_backup', view_func=self.download_from_s3, methods=['POST'])
        self.add_url_rule('/delete_backup', view_func=self.delete_backup, methods=['POST'])

        self.add_url_rule('/schedulers', view_func=self.schedulers)
        self.add_url_rule('/add_scheduler', view_func=self.add_scheduler, methods=['POST'])
        self.add_url_rule('/delete_scheduler', view_func=self.delete_scheduler, methods=['POST'])
        
        self.add_url_rule('/logs', view_func=self.logs)
        
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
                                pending_backup=self.backupper.pending_backup,
                                message=session.pop("message", None),
                                backups=backups,
                                local_size=local_size,
                                s3_size=s3_size)

    def backup_info(self):
        return self.backupper.__dict__()

    def single_backup_info(self, name: str):
        try:
            for backup in self.backupper.backups["local"]:
                if backup["name"] == name:
                    return backup.__dict__()
        except FileNotFoundError:
            session["message"] = Message(f"Backup with name {name} not found", "danger").__dict__()
            return redirect(url_for('index'))

        try:
            for backup in self.backupper.backups["s3"]:
                if backup["name"] == name:
                    info = backup.__dict__()
                    try:
                        info["size"] = size_to_human_readable(self.backupper.s3_handler.get_object_size(name + ".zip"))
                    except Exception:
                        info["size"] = "Unknown"
                    return info
        except Exception:
            session["message"] = Message(f"Backup with name {name} not found", "danger").__dict__()
            return redirect(url_for('index'))

        session["message"] = Message(f"Backup with name {name} not found", "danger").__dict__()
        return redirect(url_for('index'))

    def download_backup(self, name: str):
        try:
            for backup in self.backupper.backups["local"]:
                if backup["name"] == name:
                    return send_file(f"{self.backupper.dest_path}/{backup["name"]}.zip", as_attachment=True)
        except FileNotFoundError:
            pass

        try:
            for backup in self.backupper.backups["s3"]:
                if backup["name"] == name:
                    return redirect(self.backupper.s3_handler.create_download_link(name+".zip"))
        except Exception:
            pass

        session["message"] = Message(f"Backup with name {name} not found", "danger").__dict__()
        return redirect(url_for('index'))

    def restore_backup(self):
        name = request.form.get("backup_name", None)
        file_path = request.form.get("file_path", None)
        
        if name is None or file_path is None:
            session["message"] = Message("No backup name or file path provided", "danger").__dict__()
            self.logger.error("No backup name or file path provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            Thread(target=self.backupper.restore_backup, args=(name, file_path)).start()
            session["message"] = Message(f"Backup {name} restore requested", "info").__dict__()
            return redirect(url_for('index'))

        session["message"] = Message("Backup task is running, need to wait", "warning").__dict__()
        return redirect(url_for('index'))

    def unzip_backup(self):
        name = request.form.get("name", None)
        if name is None:
            session["message"] = Message("No backup name provided", "danger").__dict__()
            self.logger.error("No backup name provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            Thread(target=self.backupper.unzip_backup, args=(name,)).start()
            session["message"] = Message(f"Backup {name} unzip requested", "info").__dict__()
            return redirect(url_for('index'))
            # if self.backupper.unzip_backup(name):
            #     session["message"] = Message(f"Backup {name} unzipped", "success").__dict__()
            #     self.logger.info(f"Backup {name} unzipped")
            # else:
            #     session["message"] = Message(f"Error while unzipping backup {name}", "danger").__dict__()
            #     self.logger.error(f"Error while unzipping backup {name}")
            # return redirect(url_for('index'))

        session["message"] = Message("Backup task is running, need to wait", "warning").__dict__()
        return redirect(url_for('index'))

    def download_from_s3(self):
        name = request.form.get("name", None)
        if name is None:
            session["message"] = Message("No backup name provided", "danger").__dict__()
            self.logger.error("No backup name provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            Thread(target=self.backupper.download_backup_from_s3, args=(name,)).start()
            session["message"] = Message(f"Backup {name} download requested", "info").__dict__()
            return redirect(url_for('index'))

        session["message"] = Message("Backup task is running, need to wait", "warning").__dict__()
        return redirect(url_for('index'))

    def delete_backup(self):
        name = request.form.get("name", None)
        if name is None:
            session["message"] = Message("No backup name provided", "danger").__dict__()
            self.logger.error("No backup name provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            if self.backupper.delete_backup(name):
                session["message"] = Message(f"Backup {name} deleted", "success").__dict__()
                self.logger.info(f"Backup {name} deleted")
                return redirect(url_for('index'))

            session["message"] = Message(f"Error while deleting backup {name}", "danger").__dict__()
            return redirect(url_for('index'))

        session["message"] = Message("Backup task is running, need to wait", "warning").__dict__()
        return redirect(url_for('index'))

    def backup_now(self):
        if not self.backupper.pending_backup:
            self.logger.info("Backup requested")
            message = Message("Backup requested", "info")
            session["message"] = message.__dict__()
            Thread(target=self.backupper.run_backup, args=(self.backup_callback,)).start()
        else:
            self.logger.info("Backup already running")
            message = Message("Backup already running", "warning")
            session["message"] = message.__dict__()
        
        with self.app_context():
            return redirect(url_for('index'))

    def backup_callback(self, success: bool, message: str):
        message = Message(message, "success" if success else "danger")
        with self.app_context():
            return redirect(url_for('index'))

    def schedulers(self):
        tmp_schedulers = [scheduler.__dict__() for scheduler in self.schedulers_list]
        return render_template(
            'schedulers.html',
            schedulers=tmp_schedulers,
            pending_backup=self.backupper.pending_backup,
            message=session.pop("message", None))

    def add_scheduler(self):
        form = {}
        for key in request.form:
            form[key] = request.form[key]
        
        try:
            cron = Scheduler.to_CronTrigger(
                form["minute1"],
                form["hour1"],
                form["dow1"],
                form["day1"],
                form["month1"],
                timezone=str(get_localzone()))
        except ValueError as e:
            self.logger.exception("Failed to create cron trigger", exc_info=e)
            session["message"] = Message("Failed to create cron trigger", "danger").__dict__()
            return redirect(url_for('schedulers'))
        else:
            scheduler = Scheduler(self.backupper, trigger=cron)
            self.schedulers_list.append(scheduler)
            Thread(target=scheduler.start).start()
            return redirect(url_for('schedulers'))

    def delete_scheduler(self):
        id = request.form.get("sched_id", None)
        if id is None:
            session["message"] = Message("No scheduler id provided", "danger").__dict__()
            self.logger.error("No scheduler id provided")
            return redirect(url_for('schedulers'))
        for scheduler in self.schedulers_list:
            if scheduler.sched_job.id == id:
                session["message"] = Message(f"Scheduler with id {id} found and stopped", "success").__dict__()
                self.logger.info(f"Scheduler with id {id} found and stopped")
                scheduler.shutdown()
                self.schedulers_list.remove(scheduler)
                break
        else:
            session["message"] = Message(f"Scheduler with id {id} not found", "danger").__dict__()
            self.logger.error(f"Scheduler with id {id} not found")
        return redirect(url_for('schedulers'))

    def logs(self):
        for handler in self.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                log_file = handler.baseFilename
                break
        else:
            log = "No log file found"
            return render_template('logs.html', log=log)
        with open(log_file, "r") as f:
            log = f.read()
        if log == "":
            log = "No logs yet"
        return render_template('logs.html', log=log)


