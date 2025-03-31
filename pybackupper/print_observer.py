"""Print observer for debugging purposes."""

from pprint import pformat
from pybackupper.notifier import Observer, Message

class PrintObserver(Observer):
    """Concrete observer that prints notifications to the console."""

    def update(self, message: Message) -> None:
        """Print the notification message to the console."""
        print(f"PrintObserver: {message.human_timestamp}")
        print(f"PrintObserver: {message.title}")
        print(f"PrintObserver: {message.body}")
        if message.metadata:
            print(f"PrintObserver: {pformat(message.metadata, sort_dicts=True)}")      