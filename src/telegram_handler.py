"""Module for handling Telegram bot commands.
"""
import logging
import logging.config
import requests
import os
from pprint import pformat

class TelegramHandler():
    """Class for handling Telegram bot commands.
    """
    def __init__(self, token:str, chat_id:str, logger:logging.Logger=None):
        """_summary_

        Args:
            token (str): Telegram bot token.
            chat_id (str): Telegram chat id.
            logger (logging.Logger, optional): Logger to use. Defaults to None.

        Raises:
            ValueError: Exception raised when required argument has invalid value.
        """
        if logger is None:
            logging.config.fileConfig("log.conf")
            self.logger = logging.getLogger('pybackupper_logger')
        else:
            self.logger = logger
            
        if token is None or token == "":
            self.logger.error("Telegram token is not set.")
            raise ValueError("Telegram token is not set.")
        
        if chat_id is None or chat_id == "":
            self.logger.error("Telegram chat_id is not set.")
            raise ValueError("Telegram chat_id is not set.")
        
        self.token = token
        self.chat_id = chat_id
        self.logger.info("TelegramHandler initialized.")
        
    def send_message(self, message:str):
        """Sends message to Telegram chat.

        Args:
            message (str): Message to send.

        Raises:
            ValueError: Empty message.
            Exception: Failed to send message to Telegram chat.
            e: Exception raised when failed to send message to Telegram chat.
        """
        if message is None or message == "":
            self.logger.error("Message is empty.")
            raise ValueError("Message is empty.")
        
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = {
                "chat_id": self.chat_id, 
                "text": message,
                }
        
        try:
            response = requests.post(url, data=data)
            if response.status_code != 200:
                self.logger.error(f"Failed to send message to Telegram chat. Status code: {response.status_code}. Response: {response.text}")
                raise Exception(f"Failed to send message to Telegram chat. Status code: {response.status_code}")
            self.logger.info("Message sent to Telegram chat.")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            self.logger.error("Failed to send message to Telegram chat.")
            raise e
        
    def send_file(self, file_path:str):
        """Sends file to Telegram chat.

        Args:
            file_path (str): Path to file to send.

        Raises:
            ValueError: File path is empty.
            FileNotFoundError: File does not exist.
            Exception: Failed to send file to Telegram chat.
            e: Exception raised when failed to send file to Telegram chat.
        """
        if file_path is None or file_path == "":
            self.logger.error("File path is empty.")
            raise ValueError("File path is empty.")
        
        if not os.path.exists(file_path):
            self.logger.error(f"File {file_path} does not exist.")
            raise FileNotFoundError(f"File {file_path} does not exist.")
        
        url = f"https://api.telegram.org/bot{self.token}/sendDocument"
        data = {
                "chat_id": self.chat_id, 
                }
        files = {
                "document": open(file_path, "rb"),
                }
        
        try:
            response = requests.post(url, data=data, files=files)
            if response.status_code != 200:
                self.logger.error(f"Failed to send file to Telegram chat. Status code: {response.status_code}. Response: {response.text}")
                raise Exception(f"Failed to send file to Telegram chat. Status code: {response.status_code}")
            self.logger.info("File sent to Telegram chat.")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            self.logger.error("Failed to send file to Telegram chat.")
            raise e 
        
    def send_backup_info(self, hostname:str, backup_info:dict):
        """Sends backup info to Telegram chat.

        Args:
            hostname (str): Hostname.
            backup_info (dict): Backup info.

        Raises:
            ValueError: Hostname or backup info is empty.
            Exception: Failed to send message to Telegram chat.
            e: Exception raised when failed to send message to Telegram chat.
        """        
        
        if hostname is None or hostname == "":
            self.logger.error("Hostname is empty.")
            raise ValueError("Hostname is empty.")
        
        if backup_info is None or backup_info == {}:
            self.logger.error("Backup info is empty.")
            raise ValueError("Backup info is empty.")
        
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = {
                "chat_id": self.chat_id, 
                "text": f"""*PyBackUpper*\n*Hostname: {hostname}*\n\nBackup info:\n`{pformat(backup_info)}`""",
                "parse_mode": "markdown",
                }
        
        try:
            response = requests.post(url, data=data)
            if response.status_code != 200:
                self.logger.error(f"Failed to send message to Telegram chat. Status code: {response.status_code}. Response: {response.text}")
                raise Exception(f"Failed to send message to Telegram chat. Status code: {response.status_code}")
            self.logger.info("Message sent to Telegram chat.")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            self.logger.error("Failed to send message to Telegram chat.")
            raise e 

