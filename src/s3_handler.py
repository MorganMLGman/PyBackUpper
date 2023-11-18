import logging
import logging.config
import boto3
from  botocore.exceptions import ClientError
from os import walk, cpu_count
from os.path import basename, exists, join, normpath
from concurrent.futures import ThreadPoolExecutor
from time import sleep

class S3Handler:
    def __init__(self,
                bucket_name:str,
                access_key:str,
                secret_key:str,
                acl:str='public-read',
                region:str='us-east-1',
                url:str='https://s3.amazonaws.com',
                logger:logging.Logger = None):
        """Initialize the S3Handler class.

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

        self.logger = logger
        self.bucket_name = bucket_name
        self.acl = acl

        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            endpoint_url=url
        )

        if not self.test_connection():
            raise ConnectionError(f"Could not connect to bucket {self.bucket_name}")

        self.logger.debug(f"Connected to bucket {self.bucket_name}")

    @property
    def bucket_name(self) -> str:
        """Get the bucket name.

        Returns:
            str: The bucket name.
        """
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self, bucket_name:str) -> None:
        """Set the bucket name.

        Args:
            bucket_name (str): The bucket name.

        Raises:
            ValueError: If the bucket name is None or empty.
            TypeError: If the bucket name is not a string.
        """
        if bucket_name is None:
            raise ValueError("bucket_name cannot be None")

        if not isinstance(bucket_name, str):
            raise TypeError("bucket_name must be a string")

        if bucket_name == "":
            raise ValueError("bucket_name cannot be empty")

        self._bucket_name = bucket_name

    @property
    def acl(self) -> str:
        """Get the acl.

        Returns:
            str: The acl.
        """
        return self._acl

    @acl.setter
    def acl(self, acl:str) -> None:
        """Set the acl.

        Args:
            acl (str): The acl.

        Raises:
            ValueError: If the acl is None or empty.
            TypeError: If the acl is not a string.
        """
        if acl is None:
            raise ValueError("acl cannot be None")

        if not isinstance(acl, str):
            raise TypeError("acl must be a string")

        if acl == "":
            raise ValueError("acl cannot be empty")

        self._acl = acl

    @property
    def logger(self) -> logging.Logger:
        """Get the logger.

        Returns:
            logging.Logger: The logger.
        """
        return self._logger

    @logger.setter
    def logger(self, logger:logging.Logger) -> None:
        """Set the logger.

        Args:
            logger (logging.Logger): The logger.

        Raises:
            ValueError: If the logger is None.
            TypeError: If the logger is not a logging.Logger.
        """
        if logger is None:
            logging.config.fileConfig("log_dev.conf")
            self._logger = logging.getLogger('pybackupper_logger')
        else:
            if not isinstance(logger, logging.Logger):
                raise TypeError("logger must be a logging.Logger")
            self._logger = logger

    def test_connection(self) -> bool:
        """Test the connection to the bucket.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            _ = self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            self.logger.error(e, exc_info=True)
            return False
        return True

    def upload_file(self, file_path:str, object_name:str=None):
        """Upload a file to the bucket.

        Args:
            file_path (str): The file path.
            object_name (str, optional): The object name. Defaults to None.

        Raises:
            FileNotFoundError: If the file does not exist.
            error: botocore.exceptions.ClientError: If the upload fails.
        """

        if not exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist")

        if object_name is None:
            object_name = basename(file_path)
        object_name = object_name.replace('\\', '/')

        self.logger.debug(f"Uploading file {file_path} to {object_name}")
        try:
            _ = self.client.upload_file(file_path, self.bucket_name, object_name, ExtraArgs={'ACL': self.acl})
            self.logger.debug(f"File {file_path} uploaded successfully")
        except ClientError as error:
            if error.response['Error']['Code'] == 'LimitExceededException':
                self.logger.warn('API call limit exceeded; backing off and retrying in 5 seconds...')
                sleep(5)
                self.upload_file(file_path, object_name)
            else:
                self.logger.exception(error, exc_info=True)
                raise error

    def upload_directory(self, directory_path:str, object_name:str=None):
        """Upload a directory to the bucket.

        Args:
            directory_path (str): The directory path.
            object_name (str, optional): The object name. Defaults to None.

        Raises:
            FileNotFoundError: If the directory does not exist.
            error: botocore.exceptions.ClientError: If the upload fails.
        """
        if not exists(directory_path):
            raise FileNotFoundError(f"Directory {directory_path} does not exist")

        if object_name is None:
            object_name = basename(directory_path)
            
        self.logger.debug(f"Uploading directory {directory_path} to {object_name}")

        files_to_upload = []
        for path, _, files in walk(directory_path):
            dest_path = path.replace(directory_path, "")
            for file in files:
                files_to_upload.append((join(path, file), normpath(object_name + '/' + dest_path + '/' + file)))

        n_workers = cpu_count() * 2
        self.logger.debug(f"Uploading {len(files_to_upload)} files with {n_workers} workers")
        
        try:
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                for file_path, object_name in files_to_upload:
                    executor.submit(self.upload_file, file_path, object_name)
        except ClientError as error:
            self.logger.exception(error, exc_info=True)
            raise error
    
    def delete_file(self, file_name):
        try:
            _ = self.client.delete_object(Bucket=self.bucket_name, Key=file_name)
            self.logger.debug(f"File {file_name} deleted successfully")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True
    
    def delete_directory(self, directory_path):
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=directory_path)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                for content in response['Contents']:
                    if content['Key'].find('/') != -1:
                        self.logger.debug(f"Deleting file {content['Key']}")
                        executor.submit(self.delete_file, content['Key'])
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True
    
    def list_buckets(self):
        try:
            response = self.client.list_buckets()
            print(response)
            for bucket in response['Buckets']:
                self.logger.debug(f"Bucket: {bucket['Name']}")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True
    
    def list_files(self, prefix=None) -> list:
        files = []
        try:
            if prefix is None:
                response = self.client.list_objects_v2(Bucket=self.bucket_name)
            else:
                response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            for content in response['Contents']:
                if prefix is not None:
                    content['Key'] = content['Key'].replace(prefix, '')
                if content['Key'].find('/') == -1:
                    files.append(content['Key'])
        except KeyError:
            return []
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return []
        return files
    
    def list_directories(self, prefix=None) -> list:
        directories = []
        try:
            if prefix is None:
                response = self.client.list_objects_v2(Bucket=self.bucket_name, Delimiter='/')
            else:
                response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/')
            for content in response.get('CommonPrefixes', []):
                if prefix is not None:
                    content['Prefix'] = content['Prefix'].replace(prefix, '')
                directories.append(content['Prefix'].replace('/', ''))
        except KeyError:
            return []
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return []
        return directories
    
    def list_tree(self, prefix=None) -> list:
        tree = []
        try:
            if prefix is None:
                response = self.client.list_objects_v2(Bucket=self.bucket_name)
            else:
                response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            for content in response['Contents']:
                if prefix is not None:
                    content['Key'] = content['Key'].replace(prefix, '')
                tree.append(content['Key'])
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return []
        return tree
    
    def download_file(self, file_path, object_name=None) -> bool:
        if object_name is None:
            object_name = os.path.basename(file_path)
        
        try:
            _ = self.client.download_file(self.bucket_name, object_name, file_path)
            self.logger.debug(f"File {file_path} downloaded successfully")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True
    
    def download_directory(self, directory_path, object_name=None) -> bool:
        if object_name is None:
            object_name = os.path.basename(directory_path)
            
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=object_name)
            for content in response['Contents']:
                path = os.path.join(directory_path, os.path.dirname(content['Key']))
                if not os.path.exists(path):                    
                    os.makedirs(path)
                self.download_file(os.path.join(path, os.path.basename(content['Key'])), content['Key'])
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True

    def get_bucket_size(self):
        # TODO: Fix this, now it is only getting the size of the files in the root of the bucket and not the size of the bucket
        try:
            # Get size of whole bucket
            response = self.client.list_objects_v2(Bucket=self.bucket_name)
            size = sum([content['Size'] for content in response['Contents']])
        except KeyError:
            return 0
        except Exception as e:
            self.logger.error(e, exc_info=True)
            raise e
        return size
    
    def check_file_exists(self, file_name):
        try:
            _ = self.client.head_object(Bucket=self.bucket_name, Key=file_name)
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True
    
    def check_directory_exists(self, directory_path):
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=directory_path)
            for content in response['Contents']:
                if content['Key'].find('/') != -1:
                    return True
        except KeyError:
            return False
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return False
    
    def test_connection(self) -> bool:
        try:
            _ = self.client.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True

s3handler = S3Handler("***REMOVED***", "***REMOVED***", "***REMOVED***")

s3handler.upload_file("../test-target/backup_info.json")
s3handler.upload_directory("../test-target/2023_11_16_20_46_54")