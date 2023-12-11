
class PyBackUpper():
    def __init__(self, config:dict) -> None:
        """Initialize the PyBackUpper class.

        Args:
            config (dict): Configuration dictionary.
        """
        self.config = config
        logging.config.fileConfig("log_dev.conf")
        self.logger = logging.getLogger('pybackupper_logger')

        self.telegram_handler = TelegramHandler(
            token=config["telegram"]["token"],
            chat_id=config["telegram"]["chat_id"],
            logger=self.logger) if "telegram" in config else None

        self.s3_handler = S3Handler(
            bucket_name=config["s3"]["bucket"],
            access_key=config["s3"]["access_key"],
            secret_key=config["s3"]["secret_key"],
            acl=config["s3"]["acl"] if "acl" in config["s3"] else None,
            region=config["s3"]["region"] if "region" in config["s3"] else None,
            url=config["s3"]["url"] if "url" in config["s3"] else None,
            logger=self.logger) if "s3" in config else None

        self.backup_manager = BackupManager(
            src_path=config["src_path"],
            dest_path=config["dest_path"],
            raw_to_keep=config["raw_to_keep"],
            compressed_to_keep=config["compressed_to_keep"],
            s3_to_keep=config["s3_to_keep"],
            ignored=config["ignored"],
            s3_handler=self.s3_handler,
            telegram_handler=self.telegram_handler,
            )

        self.server = Server(self.backup_manager, logger=self.logger)

        self.logger.info("PyBackUpper initialized.")


if __name__ == "__main__":
    import logging
    import logging.config
    from backup_manager import BackupManager
    from s3_handler import S3Handler
    from telegram_handler import TelegramHandler
    from server import Server


