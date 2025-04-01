"""File observer for debugging purposes."""

from pprint import pformat
from pybackupper.notifier import Observer, Message

class FileObserver(Observer):
    """Concrete observer that prints notifications to the file."""
    def __init__(self, file_path: str) -> None:
        """Initialize the FileObserver with the file path."""
        self.file_path = file_path

    def update(self, message: Message) -> None:
        """Print the notification message to the file."""
        with open(self.file_path, 'a') as file:
            file.write(pformat(message, sort_dicts=False, indent=2) + '\n')