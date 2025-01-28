"""
TerraBlob (TB) Client for S3-Compatible Object Storage

This module provides a Python class, `TBClient`, for interacting with TerraBlob (TB),
an S3-compatible object storage service. TerraBlob is Uber's internal blob storage
system, designed for efficient storage and retrieval of objects mentioned publically in Uber's blog:
https://www.uber.com/blog/deduping-and-storing-images-at-uber-eats/

This client simplifies common operations such as uploading, downloading, checking the
existence of files or directories, and listing or deleting objects in the TB bucket.
Although it is originally designed for TerraBlob, users can configure it to work with
any S3-compatible storage system like Amazon S3 by providing the appropriate
service name, bucket name, and credentials.

Key Features:
- Upload files from the local system to TerraBlob or another S3-compatible storage.
- Download files from TerraBlob or another S3-compatible storage to the local system.
- Check if specific files or directories exist in the storage bucket.
- List all objects in a directory within the bucket.
- Delete objects from the bucket.

User Configuration:
- The service name, bucket name, and port number should be customized to match your
  environment.
- AWS access credentials and proxy settings are configurable via environment variables
  for secure and flexible deployment.

How to Use:
1. Update the `service_name`, `bucket_name`, and `port` parameters in the `TBClient`
   constructor or set them via environment variables.
2. Use methods like `upload_file_to_tb` and `download_file_from_tb` to perform file
   operations.
3. Refer to the method docstrings for detailed usage examples and guidelines.

Environment Variables:
- `AWS_ACCESS_KEY_ID`: Your AWS access key ID.
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key.
- `HTTP_PROXY`: Proxy configuration for connecting to the TerraBlob service.

Logging:
- This module uses Python's `logging` library to provide detailed logs for debugging
  and monitoring.

Note:
Ensure that any sensitive information, such as access credentials, is secured and not
exposed in public repositories or logs.
"""

from os import PathLike, getenv
import logging
from typing import Optional, Any, Union

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from boto3.s3.transfer import S3Transfer


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class TBClient:
    """TerraBlob (TB) client class for interacting with object storage."""

    def __init__(
        self,
        # USER CONFIGURATION REQUIRED: Set the service name.
        service_name: str = "default-service",
        # USER CONFIGURATION REQUIRED: Specify the port number for your environment.
        port: str = "12345",
        # USER CONFIGURATION REQUIRED: Replace with your storage bucket name.
        bucket_name: str = "default-bucket",
        # Timeout is specified in seconds.
        timeout: int = 60,
    ) -> None:
        super().__init__()
        self.bucket_name = bucket_name
        self.service_name = service_name
        self.port = port
        self.timeout = timeout
        self.boto3_client = self.get_boto3_client()

    def get_boto3_client(self) -> BaseClient:
        """Create a boto3 client for TerraBlob."""
        return boto3.client(
            's3',
            # USER CONFIGURATION REQUIRED: Set AWS access keys through environment variables.
            aws_access_key_id=getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=getenv("AWS_SECRET_ACCESS_KEY"),
            use_ssl=False,
            # USER CONFIGURATION REQUIRED: Update the proxy settings if needed for your environment.
            config=Config(proxies={'http': f'localhost:{self.port}'}, read_timeout=self.timeout),
        )

    def upload_file_to_tb(
        self,
        tb_file_path: str,
        local_file_path: Union[str, PathLike],
        extra_args: Optional[Any] = None,
    ) -> None:
        """Upload a file from the local system to the TerraBlob bucket."""
        transfer = S3Transfer(self.boto3_client)

        try:
            logging.debug(f"Start uploading '{local_file_path}' to {tb_file_path}")
            transfer.upload_file(
                local_file_path,
                self.bucket_name,
                # TB path can't start with a forward slash "/"
                tb_file_path.lstrip("/"),
                extra_args=extra_args,
            )
            logging.info(f"Uploaded '{local_file_path}' successfully from local system to TB path '{tb_file_path}'")
        except Exception:
            logging.warning(f"Failed to upload '{local_file_path}' to '{tb_file_path}'")
            return None
        else:
            return tb_file_path

    def download_file_from_tb(
        self,
        local_file_path: Union[str, PathLike],
        tb_file_path: str,
    ) -> Optional[str]:
        """Download a file from the TerraBlob bucket to the local system."""
        transfer = S3Transfer(self.boto3_client)

        try:
            logging.info(f"Downloading from TB file path {tb_file_path} to {local_file_path}")
            transfer.download_file(
                self.bucket_name,
                # TB path can't start with a forward slash "/"
                tb_file_path.lstrip("/"),
                local_file_path,
            )
            logging.info(f"Downloaded '{tb_file_path}' successfully from TB to local path '{local_file_path}'")
        except Exception:
            logging.warning(f"Failed to download '{tb_file_path}'")
            return None
        else:
            return local_file_path

    def check_if_file_exists(self, path: str) -> bool:
        """Check if a file exists in the TerraBlob bucket."""
        # Ensure the file name doesn't end with a '/'
        if path.endswith('/'):
            raise ValueError("File name can't end with a separator.")

        try:
            self.boto3_client.head_object(Bucket=self.bucket_name, Key=path.lstrip("/"))
        except Exception:
            return False
        else:
            return True

    def check_if_dir_exists(self, path: str) -> bool:
        """Check if a directory exists in the TerraBlob bucket."""
        # Ensure the directory name ends with a '/'
        if not path.endswith('/'):
            path += '/'

        try:
            response = self.boto3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=path.lstrip("/"), Delimiter="/")
        except Exception:
            logging.warning("Failed to check whether directory exists in TB via `list_objects_v2` call")
            return False
        else:
            # Check if 'Contents' key is found in the response dictionary
            return "Contents" in response

    def list_dir(self, path: str) -> list[str]:
        """Get all objects in the specified directory from the TerraBlob bucket."""
        # Ensure the prefix ends in '/' to avoid partial matches
        if not path.endswith('/'):
            path += '/'

        # Ensure the prefix doesn't start with '/' to avoid errors on the TB side
        if path.startswith("/"):
            path = path.lstrip("/")

        try:
            paginator = self.boto3_client.get_paginator('list_objects_v2')

            return [
                # "+1" is needed to remove the '/' at the beginning of the filename.
                obj["Key"][len(path) + 1:]
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=path, Delimiter="/")
                for obj in page['Contents']
            ]
        except Exception:
            logging.warning(f"Failed to list the `{path}` directory in TB via `list_objects_v2` call")
            return []

    def delete_from_tb(self, path: str):
        """Delete an object from the TerraBlob bucket."""
        # Ensure the prefix doesn't start with '/' to avoid errors on the TB side
        if path.startswith("/"):
            path = path.lstrip("/")

        try:
            self.boto3_client.delete_object(Bucket=self.bucket_name, Key=path)
        except Exception:
            logging.error(f"Deleting object with key '{path}' from TB failed")
            return False
        else:
            return True
