from os import PathLike
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
    """TB client class."""

    def __init__(
        self,
        service_name: str = "crisp",
        bucket_name: str = "crisp-storage",
        # Timeout is specified in seconds
        timeout: int = 60,
    ) -> None:
        super().__init__()
        self.bucket_name = bucket_name
        self.service_name = service_name
        self.timeout = timeout
        self.boto3_client = self.get_boto3_client()

    def get_boto3_client(self) -> BaseClient:
        """Create boto3 client"""
        return boto3.client('s3', config=Config(read_timeout=self.timeout))

    def upload_file_to_tb(
        self,
        tb_file_path: str,
        local_file_path: Union[str, PathLike],
        extra_args: Optional[Any] = None,
    ) -> None:
        """Upload a file from local system to TB."""
        transfer = S3Transfer(self.boto3_client)

        try:
            logging.debug(f"Start uploading '{local_file_path}' to {tb_file_path}")
            transfer.upload_file(
                local_file_path,
                self.bucket_name,
                # TB path can't start with forward slash "/"
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
        """Download a file from TB to local system."""
        transfer = S3Transfer(self.boto3_client)

        try:
            logging.info(f"Downloading from TB file path {tb_file_path} to {local_file_path}")
            transfer.download_file(
                self.bucket_name,
                # TB path can't start with forward slash "/"
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
        """Check if file exists in TB."""
        # Ensure the directory name doesn't end with a '/'
        if path.endswith('/'):
           raise ValueError("File name can't end with a separator.")

        try:
            self.boto3_client.head_object(Bucket=self.bucket_name, Key=path.lstrip("/"))
        except Exception:
            return False
        else:
            return True

    def check_if_dir_exists(self, path: str) -> bool:
        """Check if directory exists in TB."""
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
        """Get all objects in specified directory from TB."""
        # Ensure the prefix ends in '/' to avoid partial matches
        if not path.endswith('/'):
            path += '/'

        # Ensure the prefix doesn't start with '/' to avoid errors on TB side
        if path.startswith("/"):
            path = path.lstrip("/")

        try:
            paginator = self.boto3_client.get_paginator('list_objects_v2')

            return [
                # "+1" is needed to remove the '/' at the beginning of the filename.
                # if we list_dir() "/a/b/c" where there are files x, y, z, we get /x /y /z.
                # hence, we strip / to get x, y, z
                obj["Key"][len(path) + 1:]
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=path, Delimiter="/")
                for obj in page['Contents']
            ]
        except Exception:
            logging.warning(f"Failed to list the `{path}` directory in TB via `list_objects_v2` call")
            return []

    def delete_from_tb(self, path: str):
        """Delete from s3."""
        # Ensure the prefix doesn't start with '/' to avoid errors on TB side
        if path.startswith("/"):
            path = path.lstrip("/")

        try:
            self.boto3_client.delete_object(Bucket=self.bucket_name, Key=path)
        except Exception:
            logging.error(f"Deleting object with key '{path}' from TB failed")
            return False
        else:
            return True
