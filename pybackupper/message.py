from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from pybackupper.tools import timestamp_to_human_readable

class MessageType(Enum):
    """Enum for message types."""
    BACKUP_START = auto()
    BACKUP_SUCCESS = auto()
    BACKUP_FAILURE = auto()
    BACKUP_INFO_SAVE = auto()
    BACKUP_INFO_FAILURE = auto()
    S3_UPLOAD_SUCCESS = auto()
    S3_UPLOAD_PROGRESS = auto()
    S3_UPLOAD_FAILURE = auto()
    S3_DELETE_OLD = auto()

@dataclass
class Message:
    """Message class to hold message data."""
    type: MessageType
    body: str = None
    title: str = None
    timestamp: float = None
    metadata: dict = None       
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().timestamp()
        self.human_timestamp = timestamp_to_human_readable(self.timestamp)
        
    def concat(self, other) -> None:
        """Concatenate two messages."""
        if not isinstance(other, Message):
            raise TypeError(f"Cannot add {type(other)} to Message")
        
        self.message_type = other.message_type
        
        self.title = other.title or self.title
        self.body = f"{self.body} {other.body}"
        
        self.timestamp = max(self.timestamp, other.timestamp or 0)
        self.human_timestamp = timestamp_to_human_readable(self.timestamp)
        
        self.metadata.update(other.metadata or {})