"""Backup Notifier Module"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pybackupper.tools import timestamp_to_human_readable
from pybackupper.logger import logger

@dataclass
class Message:
    """Message class to hold message data."""
    title: str
    body: str
    timestamp: float = None
    metadata: dict = None       
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().timestamp()
        self.human_timestamp = timestamp_to_human_readable(self.timestamp)
    
class Observer(ABC):
    """Abstract base class for observers."""

    @abstractmethod
    def update(self, message: Message) -> None:
        """Update method to be called when a notification is received."""
        pass

class Topic(ABC):
    """Abstract base class for topics."""

    @abstractmethod
    def attach(self, observer) -> None:
        """Attach an observer to the topic."""
        pass

    @abstractmethod
    def detach(self, observer) -> None:
        """Detach an observer from the topic."""
        pass
    
    @abstractmethod
    def notify(self, message: Message) -> None:
        """Notify all observers with a message."""
        pass
    
class BackupStartTopic(Topic):
    """Concrete topic for backup start notifications."""

    def __init__(self) -> None:
        logger.debug("Initializing BackupStartTopic")
        self._observers = []

    def attach(self, observer) -> None:
        """Attach an observer to the topic."""
        logger.debug(f"Attaching observer: {observer}")
        self._observers.append(observer)

    def detach(self, observer) -> None:
        """Detach an observer from the topic."""
        logger.debug(f"Detaching observer: {observer}")
        self._observers.remove(observer)

    def notify(self, message: Message) -> None:
        """Notify all observers with a message."""
        for observer in self._observers:
            logger.debug(f"Updating observer: {observer} with message: {message}")
            observer.update(message)
    
class BackupSuccessTopic(Topic):
    """Concrete topic for backup success notifications."""

    def __init__(self) -> None:
        logger.debug("Initializing BackupSuccessTopic")
        self._observers = []

    def attach(self, observer) -> None:
        """Attach an observer to the topic."""
        logger.debug(f"Attaching observer: {observer}")
        self._observers.append(observer)

    def detach(self, observer) -> None:
        """Detach an observer from the topic."""
        logger.debug(f"Detaching observer: {observer}")
        self._observers.remove(observer)

    def notify(self, message: Message) -> None:
        """Notify all observers with a message."""
        for observer in self._observers:
            logger.debug(f"Updating observer: {observer} with message: {message}")
            observer.update(message)
            
class BackupFailureTopic(Topic):
    """Concrete topic for backup failure notifications."""

    def __init__(self) -> None:
        logger.debug("Initializing BackupFailureTopic")
        self._observers = []

    def attach(self, observer) -> None:
        """Attach an observer to the topic."""
        logger.debug(f"Attaching observer: {observer}")
        self._observers.append(observer)

    def detach(self, observer) -> None:
        """Detach an observer from the topic."""
        logger.debug(f"Detaching observer: {observer}")
        self._observers.remove(observer)

    def notify(self, message: Message) -> None:
        """Notify all observers with a message."""
        for observer in self._observers:
            logger.debug(f"Updating observer: {observer} with message: {message}")
            observer.update(message)