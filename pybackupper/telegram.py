"""Telegram class."""

import requests
from os.path import exists, isfile
from pybackupper.logger import logger, get_last_log_file_path
from pybackupper.singleton import Singleton
from pybackupper.message import Message, MessageType
from pybackupper.backup_notifier import Observer

# TODO: On BACKUP_FAILURE send message with log file

class SingletonObserverMeta(type(Observer), Singleton):
    """Metaclass that combines the Observer metaclass with Singleton."""
    pass

class Telegram(Observer, metaclass=SingletonObserverMeta):
    """Telegram class."""
    def __init__(self, token:str, chat_id:str) -> None:
        """Initializes Telegram class.

        Args:
            token (str): Telegram bot token.
            chat_id (str): Telegram chat id.
            logger (logging.Logger, optional): Logger. Defaults to None.
        """
        self.token = token
        self.chat_id = chat_id
        if not self.test_connection():
            logger.error("Telegram initialization failed.")
            raise ConnectionError("Telegram initialization failed.")
        logger.debug("Telegram initialized.")

    def test_connection(self) -> bool:
        """Tests connection to Telegram bot.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        url = f"https://api.telegram.org/bot{self.token}/getMe"
        try:
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                logger.error(
                    f"Telegram connection test failed. Status code: {response.status_code}.")
                return False

            if not response.json()['ok']:
                logger.error(
                    f"Telegram connection test failed. Status code: {response.status_code}. "\
                    f"Response: {response.json()}.")
                return False

            logger.debug("Telegram connection test successful.")
            return True
        except Exception as e:
            logger.error(f"Telegram connection test failed. Exception: {e}.")
            return False
    
    def update(self, message:Message) -> None:
        """Update method to be called when a notification is received.

        Args:
            message (Message): Message object containing notification data.
        """
        logger.debug(f"Telegram update called with message: {dict(message)}")            
        
        if message.type in (MessageType.BACKUP_FAILURE, MessageType.BACKUP_INFO_FAILURE):
            log_path = get_last_log_file_path()
            if log_path:
                self.send_file(
                    file_path=log_path,
                    caption=f"Log file for {message.type.name} message.",
                    silent=False,
                    )
        
        self.send_message(
            message.body, 
            silent=message.metadata.get("silent", False),
            markdown=message.metadata.get("markdown", False),
            html=message.metadata.get("html", False),
            )
    
    def send_message(self,
                    message: Message,
                    silent:bool=False,
                    markdown:bool=False,
                    html:bool=False) -> None:
        """Sends message to Telegram chat.

        Args:
            message (str): Message to send.
            silent (bool, optional): Whether to send message silently. Defaults to False.
            markdown (bool, optional): Whether to parse message as markdown. Defaults to False.
            html (bool, optional): Whether to parse message as html. Defaults to False.

        Raises:
            ValueError: Empty message.
            Exception: Failed to send message to Telegram chat.
            e: Exception raised when failed to send message to Telegram chat.
        """
        if message is None or message == "":
            logger.error("Message is empty.")
            raise ValueError("Message is empty.")

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = {
                "chat_id": self.chat_id, 
                "text": message,
                "disable_notification": silent,
                }

        if markdown:
            data["parse_mode"] = "MarkdownV2"

        if html:
            data["parse_mode"] = "HTML"

        if markdown and html:
            logger.error("Message can't be parsed as markdown and html at the same time.")
            raise ValueError("Message can't be parsed as markdown and html at the same time.")

        try:
            response = requests.post(url, data=data, timeout=10)
            if response.status_code != 200 or not response.json()['ok']:
                # TODO: Replace all logger with %s formatting
                logger.error(
                    "Failed to send message to Telegram chat. "\
                    "Status code: %s. Response: %s.", 
                    response.status_code, response.json())
                raise ConnectionError(
                    f"Failed to send message to Telegram chat. "\
                    f"Status code: {response.status_code}. Response: {response.json()}.")

            logger.debug("Message sent to Telegram chat.")
        except Exception as e:
            logger.exception(e, exc_info=True)
            logger.exception("Failed to send message to Telegram chat.")
            raise e

    def send_file(self, file_path:str, caption:str=None, silent:bool=False) -> None:
        """Sends file to Telegram chat.

        Args:
            file_path (str): Path to file to send.
            caption (str, optional): Caption for file. Defaults to None.
            silent (bool, optional): Whether to send message silently. Defaults to False.

        Raises:
            ValueError: Empty file path.
            FileNotFoundError: File does not exist.
            Exception: Failed to send file to Telegram chat.
            e: Exception raised when failed to send file to Telegram chat.
        """
        if file_path is None or file_path == "":
            logger.error("File path is empty.")
            raise ValueError("File path is empty.")

        if not exists(file_path):
            logger.error(f"File {file_path=} does not exist.")
            raise FileNotFoundError(f"File {file_path=} does not exist.")

        if not isfile(file_path):
            logger.error(f"File {file_path=} is not a file.")
            raise FileNotFoundError(f"File {file_path=} is not a file.")

        if caption is not None and caption == "":
            logger.error("Caption is provided, but is empty.")
            raise ValueError("Caption is provided, but is empty.")

        url = f"https://api.telegram.org/bot{self.token}/sendDocument"
        data = {
                "chat_id": self.chat_id, 
                "disable_notification": silent,
                }

        if caption is not None:
            data["caption"] = caption

        try:
            with open(file_path, "rb") as file:
                response = requests.post(url, data=data, files={"document": file}, timeout=10)
            if response.status_code != 200 or not response.json()['ok']:
                logger.error(
                    "Failed to send file to Telegram chat. "\
                    f"Status code: {response.status_code}. Response: {response.json()}.")
                raise ConnectionError(
                    "Failed to send file to Telegram chat. "\
                    f"Status code: {response.status_code}. Response: {response.json()}.")

            logger.debug("File sent to Telegram chat.")
        except Exception as e:
            logger.exception(e, exc_info=True)
            logger.exception("Failed to send file to Telegram chat.")
            raise e
