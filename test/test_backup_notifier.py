import unittest
import logging
from datetime import datetime
from pybackupper.logger import logger
from pybackupper.notifier import Observer, Message, BackupStartTopic, BackupSuccessTopic, BackupFailureTopic

class TestObserver(Observer):
    def __init__(self):
        self.received_messages = []

    def update(self, message: Message):
        self.received_messages.append(message)

class TestBackupNotifier(unittest.TestCase):
    
    def setUp(self):
        logger.setLevel(logging.CRITICAL)
        self.observer = TestObserver()
        self.start_topic = BackupStartTopic()
        self.success_topic = BackupSuccessTopic()
        self.failure_topic = BackupFailureTopic()
        
    def test_attach_observer_to_start_topic(self):
        self.start_topic.attach(self.observer)
        self.assertIn(self.observer, self.start_topic._observers)
        
    def test_detach_observer_from_start_topic(self):
        self.start_topic.attach(self.observer)
        self.start_topic.detach(self.observer)
        self.assertNotIn(self.observer, self.start_topic._observers)
    
    def test_attach_observer_to_success_topic(self):
        self.success_topic.attach(self.observer)
        self.assertIn(self.observer, self.success_topic._observers)
        
    def test_detach_observer_from_success_topic(self):
        self.success_topic.attach(self.observer)
        self.success_topic.detach(self.observer)
        self.assertNotIn(self.observer, self.success_topic._observers)
        
    def test_attach_observer_to_failure_topic(self):
        self.failure_topic.attach(self.observer)
        self.assertIn(self.observer, self.failure_topic._observers)
        
    def test_detach_observer_from_failure_topic(self):  
        self.failure_topic.attach(self.observer)
        self.failure_topic.detach(self.observer)
        self.assertNotIn(self.observer, self.failure_topic._observers)
        
    def test_notify_observer_from_start_topic(self):
        message = Message("Backup started", "Backup process has started.", None)
        self.start_topic.attach(self.observer)
        self.start_topic.notify(message)
        self.assertIn(message, self.observer.received_messages)
        self.assertEqual(len(self.observer.received_messages), 1)
        
    def test_notify_observer_from_success_topic(self):
        message = Message("Backup succeeded", "Backup process completed successfully.", None)
        self.success_topic.attach(self.observer)
        self.success_topic.notify(message)
        self.assertIn(message, self.observer.received_messages)
        self.assertEqual(len(self.observer.received_messages), 1)
        
    def test_notify_observer_from_failure_topic(self):  
        message = Message("Backup failed", "Backup process failed.", None)
        self.failure_topic.attach(self.observer)
        self.failure_topic.notify(message)
        self.assertIn(message, self.observer.received_messages)
        self.assertEqual(len(self.observer.received_messages), 1)
        
    def test_message_content(self):
        message = Message("Backup started", "Backup process has started.", None)
        self.start_topic.attach(self.observer)
        self.start_topic.notify(message)
        self.assertEqual(self.observer.received_messages[0].title, "Backup started")
        self.assertEqual(self.observer.received_messages[0].body, "Backup process has started.")
        
    def test_message_content_is_string(self):
        message = Message("Backup started", "Backup process has started.", None)
        self.start_topic.attach(self.observer)
        self.start_topic.notify(message)
        self.assertIsInstance(self.observer.received_messages[0].title, str)
        self.assertIsInstance(self.observer.received_messages[0].body, str)
        
    def test_message_timestamp(self):
        message = Message("Backup started", "Backup process has started.", timestamp=datetime.now().timestamp())
        self.start_topic.attach(self.observer)
        self.start_topic.notify(message)
        self.assertEqual(self.observer.received_messages[0].timestamp, message.timestamp)
        self.assertIsInstance(self.observer.received_messages[0].timestamp, float)
    
    def test_message_human_timestamp(self):
        message = Message("Backup started", "Backup process has started.", timestamp=datetime.now().timestamp())
        self.start_topic.attach(self.observer)
        self.start_topic.notify(message)
        self.assertIsInstance(self.observer.received_messages[0].human_timestamp, str)
        self.assertEqual(self.observer.received_messages[0].human_timestamp, datetime.fromtimestamp(message.timestamp).strftime("%Y-%m-%d %H:%M:%S"))
        
    def test_message_metadata(self):
        metadata = {"key": "value"}
        message = Message("Backup started", "Backup process has started.", metadata=metadata)
        self.start_topic.attach(self.observer)
        self.start_topic.notify(message)
        self.assertEqual(self.observer.received_messages[0].metadata, metadata)
        self.assertIsInstance(self.observer.received_messages[0].metadata, dict)
        
    def test_message_no_metadata(self):
        message = Message("Backup started", "Backup process has started.")
        self.start_topic.attach(self.observer)
        self.start_topic.notify(message)
        self.assertIsNone(self.observer.received_messages[0].metadata)