
# class PyBackUpper():
#     def __init__(self, config:dict) -> None:
#         """Initialize the PyBackUpper class.

#         Args:
#             config (dict): Configuration dictionary.
#         """
#         self.config = config
#         logging.config.fileConfig("log_dev.conf")
#         self.logger = logging.getLogger('pybackupper_logger')

#         self.telegram_handler = Telegram(
#             token=config["telegram"]["token"],
#             chat_id=config["telegram"]["chat_id"],
#         )if "telegram" in config else None

#         # rozbić logikę, przenieś zarządanie usuwaniem 
#         self.s3_handler = S3Manager(
#             bucket_name=config["s3"]["bucket"],
#             access_key=config["s3"]["access_key"],
#             secret_key=config["s3"]["secret_key"],
#             acl=config["s3"]["acl"] if "acl" in config["s3"] else None,
#             region=config["s3"]["region"] if "region" in config["s3"] else None,
#             url=config["s3"]["url"] if "url" in config["s3"] else None
#         ) if "s3" in config else None

#         self.backup_manager = BackupManager(
#             src_path=config["src_path"],
#             dest_path=config["dest_path"],
#             raw_to_keep=config["raw_to_keep"],
#             compressed_to_keep=config["compressed_to_keep"],
#             s3_to_keep=config["s3_to_keep"],
#             ignored=config["ignored"]
#         )

#         self.server = Server(self.backup_manager, logger=self.logger)

#         self.logger.info("PyBackUpper initialized.")

# def main():

#     with open("../test-appconfig/config.json", "r") as file:
#         config = json_load(file)
    
#     pybackupper = PyBackUpper(config)
#     pybackupper.server.run()

# if __name__ == "__main__":
#     import logging
#     import logging.config
#     from json import load as json_load
#     from backup_manager import BackupManager
#     from pybackupper.s3_manager import S3Manager
#     from pybackupper.telegram import Telegram
#     from server import Server

#     main()


from pybackupper.logger import logger
from pybackupper.backup_manager import BackupManager
from pybackupper.s3_manager import S3Manager
from pybackupper.notifier import backupTopic, s3Topic, flaskTopic
from pybackupper.file_observer import FileObserver
from os.path import normpath

# fileObserver = FileObserver("target/backup.log")
# backupTopic.attach(fileObserver)

backupmanager = BackupManager()
backupmanager.initialize(
    src_path=normpath("source"),
    dest_path=normpath("target"),
    ignored=None,
    raw_to_keep=2,
    compressed_to_keep=3
)


s3Topic.attach(backupmanager)
# flaskTopic.attach(fileObserver)

backupmanager.run_backup()