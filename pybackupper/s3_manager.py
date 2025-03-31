"""S3Manager class."""
from os import walk, cpu_count, makedirs
from os.path import basename, exists, join, normpath, dirname
from concurrent.futures import ThreadPoolExecutor
from time import sleep
import boto3
from botocore.exceptions import ClientError
from pybackupper.logger import logger
from pybackupper.tools import size_to_human_readable
from pybackupper.singleton import Singleton
from pybackupper.message import Message, MessageType
from pybackupper.backup_notifier import Observer

class SingletonS3ManagerMeta(type(Observer), Singleton):
    """Metaclass that combines the Observer metaclass with Singleton."""

class S3Manager(Observer, metaclass=SingletonS3ManagerMeta):
    """S3Manager class."""
    def __init__(self,
                bucket_name:str,
                access_key:str,
                secret_key:str,
                acl:str='public-read',
                region:str='us-east-1',
                url:str='https://s3.amazonaws.com'
                ):
        """Initialize the S3Manager class.

        Args:
            bucket_name (str): Bucket name.
            access_key (str): Access key.
            secret_key (str): Secret key.
            acl (str, optional): ACL. Defaults to 'public-read'.
            region (str, optional): Region. Defaults to 'us-east-1'.
            url (_type_, optional): URL. Defaults to 'https://s3.amazonaws.com'.
            logger (logging.Logger, optional):  Logger. Defaults to None.

        Raises:
            ConnectionError: If the connection to the bucket fails.
        """

        self.bucket_name = bucket_name
        self.acl = acl

        self.bucket = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            endpoint_url=url
        ).Bucket(self.bucket_name)

        if not self.test_connection():
            raise ConnectionError(f"Could not connect to bucket {self.bucket_name}")

        logger.debug(f"Connected to bucket {self.bucket_name}")

    def test_connection(self) -> bool:
        """Test the connection to the bucket.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        logger.debug(f"Testing connection to bucket {self.bucket_name}")
        try:
            _ = self.bucket.meta.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            logger.exception(e, exc_info=True)
            return False
        return True

    def update(self, message:Message) -> None:
        """Update method to be called when a notification is received.

        Args:
            message (Message): Message object containing notification data.
        """
        logger.debug(f"S3Manager update called with message: {message}")
        if message.type == MessageType.BACKUP_SUCCESS:
            self.upload_file(message.file_path, message.file_name)

    def upload_file(self, file_path:str, object_name:str=None):
        """Upload a file to the bucket.

        Args:
            file_path (str): The file path.
            object_name (str, optional): The object name. Defaults to None.

        Raises:
            FileNotFoundError: If the file does not exist.
            error: botocore.exceptions: If the upload fails.
        """

        if not exists(file_path):
            logger.error(f"File {file_path} does not exist")
            raise FileNotFoundError(f"File {file_path} does not exist")

        if object_name is None:
            object_name = basename(file_path)
        object_name = object_name.replace('\\', '/')

        logger.debug(f"Uploading file {file_path} to {object_name}")
        try:
            _ = self.bucket.upload_file(file_path, object_name, ExtraArgs={'ACL': self.acl})
            logger.debug(f"File {file_path} uploaded successfully")
        except ClientError as error:
            if error.response['Error']['Code'] == 'LimitExceededException':
                logger.warn(
                    'API call limit exceeded; backing off and retrying in 5 seconds...')
                sleep(5)
                self.upload_file(file_path, object_name)
            else:
                logger.exception(error, exc_info=True)
                raise error

    def upload_directory(self, directory_path:str, object_name:str=None):
        """Upload a directory to the bucket.

        Args:
            directory_path (str): The directory path.
            object_name (str, optional): The object name. Defaults to None.

        Raises:
            FileNotFoundError: If the directory does not exist.
            error: botocore.exceptions: If the upload fails.
        """
        if not exists(directory_path):
            logger.error(f"Directory {directory_path} does not exist")
            raise FileNotFoundError(f"Directory {directory_path} does not exist")

        if object_name is None:
            object_name = basename(directory_path)

        logger.debug(f"Uploading directory {directory_path} to {object_name}")

        files_to_upload = []
        for path, _, files in walk(directory_path):
            dest_path = path.replace(directory_path, "")
            for file in files:
                files_to_upload.append((join(path, file),
                                        normpath(object_name + '/' +
                                                    dest_path + '/' + file)))

        n_workers = cpu_count() * 2
        logger.debug(f"Uploading {len(files_to_upload)} files with {n_workers} workers")

        try:
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                for file_path, file_name in files_to_upload:
                    executor.submit(self.upload_file, file_path, file_name)
        except ClientError as error:
            logger.exception(error, exc_info=True)
            raise error

    def delete_file(self, file_name:str) -> None:
        """Delete a file from the bucket.

        Args:
            file_name (str): The file name.

        Raises:
            ValueError: If the file name is None or empty.
            TypeError: If the file name is not a string.
            e: botocore.exceptions: If the delete fails.
        """
        if file_name is None:
            logger.error("file_name cannot be None")
            raise ValueError("file_name cannot be None")

        if not isinstance(file_name, str):
            logger.error("file_name must be a string")
            raise TypeError("file_name must be a string")

        if file_name == "":
            logger.error("file_name cannot be empty")
            raise ValueError("file_name cannot be empty")

        try:
            _ = self.bucket.delete_objects(Delete={'Objects': [{'Key': file_name}]})
            logger.debug(f"File {file_name} deleted successfully")
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e

    def delete_directory(self, directory_path:str) -> None:
        """Delete a directory from the bucket.

        Args:
            directory_path (str): The directory path.

        Raises:
            ValueError: If the directory path is None or empty.
            TypeError: If the directory path is not a string.
            e: botocore.exceptions: If the delete fails.
        """
        if directory_path is None:
            logger.error("directory_path cannot be None")
            raise ValueError("directory_path cannot be None")

        if not isinstance(directory_path, str):
            logger.error("directory_path must be a string")
            raise TypeError("directory_path must be a string")

        if directory_path == "":
            logger.error("directory_path cannot be empty")
            raise ValueError("directory_path cannot be empty")

        if directory_path[-1] != '/':
            logger.debug(f"Adding '/' to directory_path {directory_path}")
            directory_path += '/'

        try:
            _ = self.bucket.objects.filter(Prefix=directory_path).delete()
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e

    def list_buckets(self) -> list:
        """List all the buckets.

        Returns:
            list: The list of buckets.

        Raises:
            e: botocore.exceptions: If the list fails.
        """
        logger.debug("Listing buckets")
        try:
            return [bucket["Name"] for bucket in self.bucket.meta.client.list_buckets()['Buckets']]
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e

    def list_files(self, prefix:str=None) -> list:
        """List all the files in the bucket.

        Args:
            prefix (str, optional): The prefix. Defaults to None.

        Returns:
            list: The list of files.

        Raises:
            e: botocore.exceptions: If the list fails.
        """
        logger.debug(f"Listing files in bucket {self.bucket_name}")
        files = []
        if prefix is not None and prefix[-1] != '/':
            logger.debug(f"Adding '/' to prefix {prefix}")
            prefix += '/'
        try:
            if prefix is None:
                response = self.bucket.meta.client.list_objects_v2(
                    Bucket=self.bucket_name)
            else:
                response = self.bucket.meta.client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=prefix)
            for content in response['Contents']:
                if prefix is not None:
                    content['Key'] = content['Key'].replace(prefix, '')
                if content['Key'].find('/') == -1:
                    files.append(content['Key'])
        except KeyError:
            return []
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e
        return files

    def list_directories(self, prefix:str=None) -> list:
        """List all the directories in the bucket.

        Args:
            prefix (str, optional): The prefix. Defaults to None.

        Returns:
            list: The list of directories.

        Raises:
            e: botocore.exceptions: If the list fails.
        """
        logger.debug(f"Listing directories in bucket {self.bucket_name}")
        directories = []
        if prefix is not None and prefix[-1] != '/':
            logger.debug(f"Adding '/' to prefix {prefix}")
            prefix += '/'
        try:
            if prefix is None:
                response = self.bucket.meta.client.list_objects_v2(
                    Bucket=self.bucket_name, Delimiter='/')
            else:
                response = self.bucket.meta.client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=prefix, Delimiter='/')
            for content in response.get('CommonPrefixes', []):
                if prefix is not None:
                    content['Prefix'] = content['Prefix'].replace(prefix, '')
                directories.append(content['Prefix'].replace('/', ''))
        except KeyError:
            return []
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e
        return directories

    def download_file(self, object_name:str, save_path:str) -> None:
        """Download a file from the bucket.

        Args:
            object_name (str): The object name.
            save_path (str): The save path.

        Raises:
            ValueError: If the object name or save path is None or empty.
            TypeError: If the object name or save path is not a string.
            error: botocore.exceptions: If the download fails.
        """
        if object_name is None:
            logger.error("object_name cannot be None")
            raise ValueError("object_name cannot be None")

        if not isinstance(object_name, str):
            logger.error("object_name must be a string")
            raise TypeError("object_name must be a string")

        if object_name == "":
            logger.error("object_name cannot be empty")
            raise ValueError("object_name cannot be empty")

        if save_path is None:
            logger.error("save_path cannot be None")
            raise ValueError("save_path cannot be None")

        if not isinstance(save_path, str):
            logger.error("save_path must be a string")
            raise TypeError("save_path must be a string")

        if save_path == "":
            logger.error("save_path cannot be empty")
            raise ValueError("save_path cannot be empty")

        save_path = normpath(save_path)

        if not exists(dirname(save_path)):
            logger.debug(f"Creating directory {dirname(save_path)}")
            makedirs(dirname(save_path))

        logger.debug(f"Downloading file {object_name} to {save_path}")
        try:
            with open(save_path, 'wb') as f:
                self.bucket.download_fileobj(object_name, f)
        except ClientError as error:
            logger.exception(error, exc_info=True)
            raise error

        logger.debug(f"File {object_name} downloaded successfully")

    def download_directory(self, object_name:str, save_path:str) -> None:
        """Download a directory from the bucket.

        Args:
            object_name (str): The object name.
            save_path (str): The save path.

        Raises:
            ValueError: If the object name or save path is None or empty.
            TypeError: If the object name or save path is not a string.
            error: botocore.exceptions: If the download fails.
        """
        if object_name is None:
            logger.error("object_name cannot be None")
            raise ValueError("object_name cannot be None")

        if not isinstance(object_name, str):
            logger.error("object_name must be a string")
            raise TypeError("object_name must be a string")

        if object_name == "":
            logger.error("object_name cannot be empty")
            raise ValueError("object_name cannot be empty")

        if save_path is None:
            logger.error("save_path cannot be None")
            raise ValueError("save_path cannot be None")

        if not isinstance(save_path, str):
            logger.error("save_path must be a string")
            raise TypeError("save_path must be a string")

        if save_path == "":
            logger.error("save_path cannot be empty")
            raise ValueError("save_path cannot be empty")

        save_path = normpath(save_path)

        if not exists(save_path):
            logger.debug(f"Creating directory {save_path}")
            makedirs(save_path)

        logger.debug(f"Downloading directory {object_name} to {save_path}")

        n_workers = cpu_count() * 2
        logger.debug(f"Downloading with {n_workers} workers")

        for file in self.list_files(object_name):
            try:
                self.download_file(object_name + '/' + file, join(save_path, file))
            except ClientError as error:
                logger.exception(error, exc_info=True)
                raise error

        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for directory in self.list_directories(object_name):
                try:
                    executor.submit(
                        self.download_directory,
                        object_name + '/' + directory,
                        join(save_path, directory))
                except ClientError as error:
                    logger.exception(error, exc_info=True)
                    raise error

        logger.debug(f"Directory {object_name} downloaded successfully")

    def get_bucket_size(self) -> int:
        """Get the bucket size.

        Returns:
            int: The bucket size.

        Raises:
            e: botocore.exceptions: If the size cannot be calculated.
        """
        logger.debug(f"Calculating size of bucket {self.bucket_name}")
        try:
            total_size = 0
            for key in self.bucket.objects.all():
                total_size += key.size
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e
        logger.debug(
            f"Size of bucket {self.bucket_name} is {size_to_human_readable(total_size)}")
        return total_size

    def get_object_size(self, object_name:str) -> int:
        """Get the object size.

        Args:
            object_name (str): The object name.

        Returns:
            int: The object size.

        Raises:
            ValueError: If the object name is None or empty.
            TypeError: If the object name is not a string.
            e: botocore.exceptions: If the size cannot be calculated.
        """
        if object_name is None:
            logger.error("object_name cannot be None")
            raise ValueError("object_name cannot be None")

        if not isinstance(object_name, str):
            logger.error("object_name must be a string")
            raise TypeError("object_name must be a string")

        if object_name == "":
            logger.error("object_name cannot be empty")
            raise ValueError("object_name cannot be empty")

        logger.debug(f"Calculating size of object {object_name}")
        try:
            total_size = 0
            for key in self.bucket.objects.all():
                if key.key.find(object_name) != -1:
                    total_size += key.size
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e
        logger.debug(f"Size of object {object_name} is {size_to_human_readable(total_size)}")
        return total_size

    def check_object_exists(self, object_name:str) -> bool:
        """Check if an object exists.

        Args:
            object_name (str): The object name.

        Returns:
            bool: True if the object exists, False otherwise.

        Raises:
            ValueError: If the object name is None or empty.
            TypeError: If the object name is not a string.
            e: botocore.exceptions: If the check fails.
        """
        if object_name is None:
            logger.error("object_name cannot be None")
            raise ValueError("object_name cannot be None")

        if not isinstance(object_name, str):
            logger.error("object_name must be a string")
            raise TypeError("object_name must be a string")

        if object_name == "":
            logger.error("object_name cannot be empty")
            raise ValueError("object_name cannot be empty")

        try:
            for key in self.bucket.objects.all():
                if key.key.find(object_name) != -1:
                    return True
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e
        return False

    def get_object_path(self, object_name:str) -> str:
        """Get the object path.

        Args:
            object_name (str): The object name.

        Returns:
            str: The object path.

        Raises:
            ValueError: If the object name is None or empty.
            TypeError: If the object name is not a string.
            e: botocore.exceptions: If the check fails.
        """
        if object_name is None:
            logger.error("object_name cannot be None")
            raise ValueError("object_name cannot be None")

        if not isinstance(object_name, str):
            logger.error("object_name must be a string")
            raise TypeError("object_name must be a string")

        if object_name == "":
            logger.error("object_name cannot be empty")
            raise ValueError("object_name cannot be empty")

        try:
            for key in self.bucket.objects.all():
                if key.key.find(object_name) != -1:
                    return key.key
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e
        return None

    def clear_bucket(self) -> None:
        """Clear the bucket.

        Raises:
            e: botocore.exceptions: If the clear fails.
        """
        logger.debug(f"Clearing bucket {self.bucket_name}")
        try:
            _ = self.bucket.objects.all().delete()
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e

    def create_download_link(self, object_name:str, expiration:int=3600) -> str:
        """Create a download link for an object.

        Args:
            object_name (str): The object name.
            expiration (int, optional): The expiration time in seconds. Defaults to 3600.

        Returns:
            str: The download link.

        Raises:
            ValueError: If the object name is None or empty.
            TypeError: If the object name is not a string.
            e: botocore.exceptions: If the link cannot be created.
        """
        if object_name is None:
            logger.error("object_name cannot be None")
            raise ValueError("object_name cannot be None")

        if not isinstance(object_name, str):
            logger.error("object_name must be a string")
            raise TypeError("object_name must be a string")

        if object_name == "":
            logger.error("object_name cannot be empty")
            raise ValueError("object_name cannot be empty")

        try:
            return self.bucket.meta.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': object_name},
                ExpiresIn=expiration)
        except ClientError as e:
            logger.exception(e, exc_info=True)
            raise e