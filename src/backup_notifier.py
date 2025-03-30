"""Backup Notifier Module"""

from logger import logger
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class Message:
    """Message class to hold message data."""
    title: str
    body: str
    metadata: dict = None       
    
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