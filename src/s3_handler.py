import boto3
import logging
import logging.config
import os
import concurrent.futures

class S3Handler:
    def __init__(self, bucket_name, access_key, secret_key, acl='public-read', region='us-east-1', url='https://s3.amazonaws.com', logger: logging.Logger = None):
        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            endpoint_url=url
        )
        self.bucket_name = bucket_name
        self.acl = acl
        
        if logger is None:
            logging.config.fileConfig("log.conf")
            self.logger = logging.getLogger('pybackupper_logger')
        else:
            self.logger = logger
    
    def upload_file(self, file_name, object_name=None):
        if object_name is None:
            object_name = os.path.basename(file_name)
        
        self.logger.debug(f"Uploading file {file_name} to {object_name}")
        try:
            _ = self.client.upload_file(file_name, self.bucket_name, object_name, ExtraArgs={'ACL': self.acl})
            self.logger.debug(f"File {file_name} uploaded successfully")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
        return True
        
    def upload_directory(self, directory_path, object_name=None):
        if object_name is None:
            object_name = os.path.basename(directory_path)
            
        self.logger.debug(f"Uploading directory {directory_path} to {object_name}")

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                for path, _, files in os.walk(directory_path):
                    dest_path = path.replace(directory_path, "")
                    for file in files:                        
                        s3file = os.path.normpath(object_name + '/' + dest_path + '/' + file)
                        local_file = os.path.join(path, file)
                        self.logger.debug(f"upload : {local_file} to target: {s3file}")
                        executor.submit(self.upload_file, local_file, s3file)
        except Exception as e:
            self.logger.error(e, exc_info=True)
            raise e
    
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