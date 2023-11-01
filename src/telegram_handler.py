import logging
import logging.config
import requests
from os.path import exists, isfile
from singleton import Singleton

class TelegramHandler(metaclass=Singleton):
    def __init__(self, token:str, chat_id:str, logger:logging.Logger=None) -> None:
        
        self.logger = logger       
        self.token = token
        self.chat_id = chat_id
        self.logger.info("TelegramHandler initialized.")
    
    @property
    def token(self) -> str:
        return self._token
    
    @token.setter
    def token(self, token:str) -> None:
        if token is None or token == "":
            self.logger.error(f"Telegram {token=} is not valid.")
            raise ValueError(f"Telegram {token=} is not valid.")
        self._token = token
        
    @property
    def chat_id(self) -> str:
        return self._chat_id
    
    @chat_id.setter
    def chat_id(self, chat_id:str) -> None:
        if chat_id is None or chat_id == "":
            self.logger.error(f"Telegram {chat_id=} is not valid.")
            raise ValueError(f"Telegram {chat_id=} is not valid.")
        self._chat_id = chat_id
          
    @property
    def logger(self) -> logging.Logger:
        return self._logger  
    
    @logger.setter
    def logger(self, logger:logging.Logger) -> None:
        if logger is None:
            logging.config.fileConfig("log_dev.conf")
            self._logger = logging.getLogger('pybackupper_logger')
        else:
            self._logger = logger
    
    def test_connection(self) -> bool:
        """Tests connection to Telegram bot.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        url = f"https://api.telegram.org/bot{self.token}/getMe"
        try:
            response = requests.get(url)
            
            if response.status_code != 200:
                self.logger.error(f"Telegram connection test failed. Status code: {response.status_code}.")
                return False
            elif response.json()['ok'] != True:
                self.logger.error(f"Telegram connection test failed. Status code: {response.status_code}. Response: {response.json()}.")
                return False
            else:
                self.logger.info("Telegram connection test successful.")
                return True
        except Exception as e:
            self.logger.error(f"Telegram connection test failed. Exception: {e}.")
            return False
        
    def send_message(self, message:str, silent:bool=False, markdown:bool=False, html:bool=False) -> None:
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
            self.logger.error("Message is empty.")
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
            self.logger.error("Message can't be parsed as markdown and html at the same time.")
            raise ValueError("Message can't be parsed as markdown and html at the same time.")        
        
        try:
            response = requests.post(url, data=data)
            if response.status_code != 200 or response.json()['ok'] != True:
                self.logger.error(f"Failed to send message to Telegram chat. Status code: {response.status_code}. Response: {response.json()}.")
                raise Exception(f"Failed to send message to Telegram chat. Status code: {response.status_code}. Response: {response.json()}.")
            
            self.logger.debug(f"Message sent to Telegram chat.")
        except Exception as e:
            self.logger.exception(e, exc_info=True)
            self.logger.exception("Failed to send message to Telegram chat.")
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
            self.logger.error("File path is empty.")
            raise ValueError("File path is empty.")
        elif not exists(file_path):
            self.logger.error(f"File {file_path=} does not exist.")
            raise FileNotFoundError(f"File {file_path=} does not exist.")
        elif not isfile(file_path):
            self.logger.error(f"File {file_path=} is not a file.")
            raise FileNotFoundError(f"File {file_path=} is not a file.")
        
        if caption is not None and caption == "":
            self.logger.error("Caption is provided, but is empty.")
            raise ValueError("Caption is provided, but is empty.")
        
        url = f"https://api.telegram.org/bot{self.token}/sendDocument"
        data = {
                "chat_id": self.chat_id, 
                "disable_notification": silent,
                }
        
        if caption is not None:
            data["caption"] = caption
        
        try:
            response = requests.post(url, data=data, files={"document": open(file_path, "rb")})
            if response.status_code != 200 or response.json()['ok'] != True:
                self.logger.error(f"Failed to send file to Telegram chat. Status code: {response.status_code}. Response: {response.json()}.")
                raise Exception(f"Failed to send file to Telegram chat. Status code: {response.status_code}. Response: {response.json()}.")
            
            self.logger.debug(f"File sent to Telegram chat.")
        except Exception as e:
            self.logger.exception(e, exc_info=True)
            self.logger.exception("Failed to send file to Telegram chat.")
            raise e
