import unittest
import sys
import os
from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError
from boto3.s3.transfer import S3Transfer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tb_client import TBClient

class TestTBClient(unittest.TestCase):

    @patch('tb_client.boto3.client')
    def setUp(self, mock_boto_client):  # noqa: ARG002
        self.client = TBClient(service_name='test', port='12345', bucket_name='test_bucket')

    def test_init(self):
        self.assertEqual(self.client.service_name, 'test')
        self.assertEqual(self.client.port, '12345')
        self.assertEqual(self.client.bucket_name, 'test_bucket')
        self.assertIsInstance(self.client.boto3_client, MagicMock)

    @patch.object(S3Transfer, 'upload_file')
    def test_upload_file_to_tb(self, mock_upload_file):
        mock_upload_file.return_value = None
        tb_file_path = 'path/to/tbfile'
        local_file_path = 'path/to/localfile'
        result = self.client.upload_file_to_tb(tb_file_path, local_file_path)
        mock_upload_file.assert_called_once_with(local_file_path, 'test_bucket', tb_file_path, extra_args=None)
        self.assertEqual(result, tb_file_path)

        mock_upload_file.side_effect = Exception("Upload failed")
        result = self.client.upload_file_to_tb(tb_file_path, local_file_path)
        self.assertIsNone(result)

    @patch.object(S3Transfer, 'download_file')
    def test_download_file_from_tb(self, mock_download_file):
        mock_download_file.return_value = None
        tb_file_path = 'path/to/tbfile'
        local_file_path = 'path/to/localfile'
        result = self.client.download_file_from_tb(local_file_path, tb_file_path)
        mock_download_file.assert_called_once_with('test_bucket', tb_file_path, local_file_path)
        self.assertEqual(result, local_file_path)

        mock_download_file.side_effect = Exception("Download failed")
        result = self.client.download_file_from_tb(local_file_path, tb_file_path)
        self.assertIsNone(result)

    def test_check_if_file_exists(self):
        self.client.boto3_client.head_object.return_value = {}
        result = self.client.check_if_file_exists('path/to/file')
        self.assertTrue(result)

        self.client.boto3_client.head_object.side_effect = ClientError({}, 'head_object')
        result = self.client.check_if_file_exists('path/to/file')
        self.assertFalse(result)

    def test_check_if_dir_exists(self):
        self.client.boto3_client.list_objects_v2.return_value = {'Contents': []}
        result = self.client.check_if_dir_exists('path/to/dir/')
        self.assertTrue(result)

        self.client.boto3_client.list_objects_v2.return_value = {}
        result = self.client.check_if_dir_exists('path/to/dir/')
        self.assertFalse(result)

        self.client.boto3_client.list_objects_v2.side_effect = Exception("List objects failed")
        result = self.client.check_if_dir_exists('path/to/dir/')
        self.assertFalse(result)

    def test_list_dir(self):
        mock_paginator = self.client.boto3_client.get_paginator.return_value
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': '/path/to/dir/file1'}, {'Key': '/path/to/dir/file2'}]},
        ]
        result = self.client.list_dir('path/to/dir/')
        self.assertEqual(result, ['file1', 'file2'])

        mock_paginator.paginate.side_effect = Exception("Pagination failed")
        result = self.client.list_dir('path/to/dir/')
        self.assertEqual(result, [])

    def test_delete_from_tb(self):
        path = 'path/to/file'
        self.client.delete_from_tb(path)
        self.client.boto3_client.delete_object.assert_called_once_with(Bucket='test_bucket', Key=path)

        self.client.boto3_client.delete_object.side_effect = ValueError("No such file")
        result = self.client.delete_from_tb(path)

        self.assertFalse(result)
