"""Module to represent the server"""

import logging
import logging.config
from secrets import token_hex
from threading import Thread
from flask_wtf.csrf import CSRFProtect
from flask.logging import default_handler
from flask import Flask, render_template, redirect, url_for, session, request, send_file
from tzlocal import get_localzone
from backup_manager import BackupManager
from scheduler import Scheduler
from tools import size_to_human_readable

class Message():
    """Class to represent a message to be displayed on the website
    """
    def __init__(self, message: str, level: str) -> None:
        """Constructor for Message class

        Args:
            message (str): Message to be displayed
            level (str): Level of the message, can be one of the following:
            primary, secondary, success, danger, warning, info, light, dark
        """
        self.message = message if isinstance(message, str) else str(message)
        self.level = level if level in ["primary",
                                        "secondary",
                                        "success",
                                        "danger",
                                        "warning",
                                        "info",
                                        "light",
                                        "dark"] else "info"

    def __str__(self) -> str:
        """String representation of the Message object

        Returns:
            str: String representation of the Message object
        """
        return f"{self.level}: {self.message}"

    def __dict__(self) -> dict:
        """Dictionary representation of the Message object

        Returns:
            dict: Dictionary representation of the Message object
        """
        return {
            "message": self.message,
            "level": self.level
        }

    def to_dict(self) -> dict:
        """Dictionary representation of the Message object

        Returns:
            dict: Dictionary representation of the Message object
        """
        return dict(self)

class Server(Flask):
    """Class to represent the server

    Args:
        Flask (Flask): Flask object
    """
    def __init__(self, backupper: BackupManager, logger: logging.Logger=None) -> None:
        """Constructor for Server class

        Args:
            backupper (BackupManager): BackupManager object
            logger (logging.Logger, optional): Logger object. Defaults to None.
        """
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
    def logger(self) -> logging.Logger:
        """Logger object

        Returns:
            logging.Logger: Logger object
        """
        return self._logger

    @logger.setter
    def logger(self, logger: logging.Logger) -> None:
        """Logger object setter

        Args:
            logger (logging.Logger): Logger object
        """
        if logger is None:
            logging.config.fileConfig("log_dev.conf")
            self._logger = logging.getLogger('pybackupper_logger')
        else:
            self._logger = logger

    def index(self) -> str:
        """Index page

        Returns:
            str: HTML page
        """
        backups_dict = self.backupper.__dict__()
        local_size = backups_dict["local_size"] if "local_size" in backups_dict else 0
        s3_size = backups_dict["s3_size"] if "s3_size" in backups_dict else 0

        backups = []
        for backup in backups_dict["backups"]["local"]:
            backups.append({
                "name": backup["name"],
                "size": backup["size"],
                "raw": backup["completed"],
                "zip": backup["compressed"],
                "s3": backup in backups_dict["backups"]["s3"],
            })

        for backup in backups_dict["backups"]["s3"]:
            if backup not in backups_dict["backups"]["local"]:
                backups.append({
                    "name": backup["name"],
                    "size": size_to_human_readable(
                        self.backupper.s3_handler.get_object_size(
                            backup["name"])),
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

    def backup_info(self) -> dict:
        """Backup info

        Returns:
            dict: Backup info
        """
        return self.backupper.__dict__()

    def single_backup_info(self, name: str) -> dict:
        """Single backup info action button

        Args:
            name (str): Backup name

        Returns:
            dict: Single backup info
        """
        try:
            for backup in self.backupper.backups["local"]:
                if backup["name"] == name:
                    return backup.__dict__()
        except FileNotFoundError:
            session["message"] = Message(f"Backup with name {name} not found", "danger").to_dict()
            return redirect(url_for('index'))

        try:
            for backup in self.backupper.backups["s3"]:
                if backup["name"] == name:
                    info = backup.__dict__()
                    try:
                        info["size"] = size_to_human_readable(
                            self.backupper.s3_handler.get_object_size(
                                name + ".zip"))
                    except Exception:
                        info["size"] = "Unknown"
                    return info
        except Exception:
            session["message"] = Message(f"Backup with name {name} not found", "danger").to_dict()
            return redirect(url_for('index'))

        session["message"] = Message(f"Backup with name {name} not found", "danger").to_dict()
        return redirect(url_for('index'))

    def download_backup(self, name: str)-> str:
        """Download backup action button

        Args:
            name (str): Backup name

        Returns:
            str: HTML page
        """
        try:
            for backup in self.backupper.backups["local"]:
                if backup["name"] == name:
                    return send_file(
                        f"""{self.backupper.dest_path}/{backup["name"]}.zip""",
                        as_attachment=True)
        except FileNotFoundError:
            pass

        try:
            for backup in self.backupper.backups["s3"]:
                if backup["name"] == name:
                    return redirect(self.backupper.s3_handler.create_download_link(name+".zip"))
        except Exception:
            pass

        session["message"] = Message(
            f"Backup with name {name} not found", "danger").to_dict()
        return redirect(url_for('index'))

    def restore_backup(self) -> str:
        """Restore backup action button

        Returns:
            str: HTML page
        """
        name = request.form.get("backup_name", None)
        file_path = request.form.get("file_path", None)

        if name is None or file_path is None:
            session["message"] = Message(
                "No backup name or file path provided", "danger").to_dict()
            self.logger.error("No backup name or file path provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            Thread(
                target=self.backupper.restore_backup,
                args=(name, file_path)).start()
            session["message"] = Message(
                f"Backup {name} restore requested", "info").to_dict()
            return redirect(url_for('index'))

        session["message"] = Message(
            "Backup task is running, need to wait", "warning").to_dict()
        return redirect(url_for('index'))

    def unzip_backup(self) -> str:
        """Unzip backup action button

        Returns:
            str: HTML page
        """
        name = request.form.get("name", None)
        if name is None:
            session["message"] = Message("No backup name provided", "danger").to_dict()
            self.logger.error("No backup name provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            Thread(target=self.backupper.unzip_backup, args=(name,)).start()
            session["message"] = Message(f"Backup {name} unzip requested", "info").to_dict()
            return redirect(url_for('index'))

        session["message"] = Message("Backup task is running, need to wait", "warning").to_dict()
        return redirect(url_for('index'))

    def download_from_s3(self) -> str:
        """Download backup from s3 action button

        Returns:
            str: HTML page
        """
        name = request.form.get("name", None)
        if name is None:
            session["message"] = Message("No backup name provided", "danger").to_dict()
            self.logger.error("No backup name provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            Thread(target=self.backupper.download_backup_from_s3, args=(name,)).start()
            session["message"] = Message(f"Backup {name} download requested", "info").to_dict()
            return redirect(url_for('index'))

        session["message"] = Message("Backup task is running, need to wait", "warning").to_dict()
        return redirect(url_for('index'))

    def delete_backup(self) -> str:
        """Delete backup action button

        Returns:
            str: HTML page
        """
        name = request.form.get("name", None)
        if name is None:
            session["message"] = Message("No backup name provided", "danger").to_dict()
            self.logger.error("No backup name provided")
            return redirect(url_for('index'))

        if not self.backupper.pending_backup:
            if self.backupper.delete_backup(name):
                session["message"] = Message(f"Backup {name} deleted", "success").to_dict()
                self.logger.info(f"Backup {name} deleted")
                return redirect(url_for('index'))

            session["message"] = Message(f"Error while deleting backup {name}", "danger").to_dict()
            return redirect(url_for('index'))

        session["message"] = Message("Backup task is running, need to wait", "warning").to_dict()
        return redirect(url_for('index'))

    def backup_now(self) -> str:
        """Backup now action button

        Returns:
            str: HTML page
        """
        if not self.backupper.pending_backup:
            self.logger.info("Backup requested")
            message = Message("Backup requested", "info")
            session["message"] = message.to_dict()
            Thread(target=self.backupper.run_backup, args=(self.backup_callback,)).start()
        else:
            self.logger.info("Backup already running")
            message = Message("Backup already running", "warning")
            session["message"] = message.to_dict()

        with self.app_context():
            return redirect(url_for('index'))

    def backup_callback(self, success: bool, message: str) -> str:
        """Backup callback

        Args:
            success (bool): If the backup was successful
            message (str): Message to be displayed

        Returns:
            str: HTML page
        """
        message = Message(message, "success" if success else "danger")
        with self.app_context():
            return redirect(url_for('index'))

    def schedulers(self) -> str:
        """Schedulers page

        Returns:
            str: HTML page
        """
        tmp_schedulers = [scheduler.__dict__() for scheduler in self.schedulers_list]
        return render_template(
            'schedulers.html',
            schedulers=tmp_schedulers,
            pending_backup=self.backupper.pending_backup,
            message=session.pop("message", None))

    def add_scheduler(self) -> str:
        """Add scheduler action button

        Returns:
            str: HTML page
        """
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
            session["message"] = Message("Failed to create cron trigger", "danger").to_dict()
            return redirect(url_for('schedulers'))

        scheduler = Scheduler(self.backupper, trigger=cron)
        self.schedulers_list.append(scheduler)
        Thread(target=scheduler.start).start()
        return redirect(url_for('schedulers'))

    def delete_scheduler(self) -> str:
        """Delete scheduler action button

        Returns:
            str: HTML page
        """
        shed_id = request.form.get("sched_id", None)
        if shed_id is None:
            session["message"] = Message("No scheduler id provided", "danger").to_dict()
            self.logger.error("No scheduler id provided")
            return redirect(url_for('schedulers'))
        for scheduler in self.schedulers_list:
            if scheduler.sched_job.id == shed_id:
                session["message"] = Message(
                    f"Scheduler with id {shed_id} found and stopped", "success").to_dict()
                self.logger.info(f"Scheduler with id {shed_id} found and stopped")
                scheduler.shutdown()
                self.schedulers_list.remove(scheduler)
                break
        else:
            session["message"] = Message(
                f"Scheduler with id {shed_id} not found", "danger").to_dict()
            self.logger.error(f"Scheduler with id {shed_id} not found")
        return redirect(url_for('schedulers'))

    def logs(self) -> str:
        """Logs page

        Returns:
            str: HTML page
        """
        for handler in self.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                log_file = handler.baseFilename
                break
        else:
            log = "No log file found"
            return render_template('logs.html', log=log)
        with open(log_file, "r", encoding="utf8") as f:
            log = f.read()
        if log == "":
            log = "No logs yet"
        return render_template('logs.html', log=log)
