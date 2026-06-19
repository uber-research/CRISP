"""Tests for storage.py - S3-compatible storage upload functionality."""

import unittest
from unittest.mock import patch, MagicMock

import crisp.common as common
from crisp.storage import (
    compressAndUpload,
    Upload,
    uploadWrapper,
    TBPathExists,
    uploadReal,
    uploadCrossRegionCalls,
)


class TestTBPathExists(unittest.TestCase):
    """Test cases for TBPathExists function."""

    @patch("crisp.storage.TBClient")
    def test_TBPathExists(self, mock_tb_client):
        mock_client_instance = mock_tb_client.return_value
        mock_client_instance.check_if_dir_exists.return_value = True

        result = TBPathExists("/mock/path")

        mock_tb_client.assert_called_once_with()
        mock_client_instance.check_if_dir_exists.assert_called_once_with(path="/mock/path")
        self.assertTrue(result)

    @patch("crisp.storage.TBClient")
    def test_TBPathExists_false(self, mock_tb_client):
        mock_client_instance = mock_tb_client.return_value
        mock_client_instance.check_if_dir_exists.return_value = False

        result = TBPathExists("/mock/missing")

        self.assertFalse(result)


class TestUploadWrapper(unittest.TestCase):
    """Test cases for uploadWrapper function."""

    @patch("crisp.storage.common.templateHandler")
    def test_uploadWrapper(self, mock_template_handler):
        config = MagicMock()
        result_queue = MagicMock()

        result = uploadWrapper(config, result_queue)

        mock_template_handler.assert_called_once_with(
            message="upload step",
            realHandler=uploadReal,
            preStart=None,
            postFinish=None,
            c=config,
            resultQ=result_queue,
        )
        self.assertEqual(result, mock_template_handler.return_value)


class TestUploadReal(unittest.TestCase):
    """Test cases for the simplified uploadReal function."""

    @patch("crisp.storage.TBPATH", "/mock/tbpath/")
    @patch("crisp.storage.DATE_TIME", "2023-01-01T00:00:00")
    @patch("crisp.storage.CRISP_SECONDARY_TBPATH", "/mock/crisp_secondary_tbpath/")
    @patch("crisp.storage.CRISP_SECONDARY_DATE_TIME", "2023-01-02T00:00:00")
    @patch("crisp.storage.Upload")
    @patch("crisp.storage.compressAndUpload")
    @patch("crisp.storage.TBPathExists")
    def test_uploadReal_always_uploads(
        self,
        mock_tb_path_exists,
        mock_compress_and_upload,
        mock_upload,
    ):
        config = MagicMock()
        config.serviceName = "mock_service"
        config.operationName = "mock_operation"
        config.tracesDir = "/mock/traces"
        config.filesToUpload = ["/mock/file1", "/mock/file2"]

        # Path is absent so RI upload proceeds
        mock_tb_path_exists.return_value = False

        result = uploadReal(config)

        self.assertEqual(result, 0)

        mock_compress_and_upload.assert_called_once_with(
            config.serviceName,
            config.operationName,
            config.tracesDir,
            "/mock/tbpath/",
            "2023-01-01T00:00:00",
            publishAsLatest=False,
        )

        # Upload called for both TBPATH and CRISP_SECONDARY_TBPATH
        self.assertEqual(mock_upload.call_count, 2)
        mock_upload.assert_any_call(
            config.serviceName,
            config.operationName,
            config.filesToUpload,
            "/mock/tbpath/",
            "2023-01-01T00:00:00",
        )
        mock_upload.assert_any_call(
            config.serviceName,
            config.operationName,
            config.filesToUpload,
            "/mock/crisp_secondary_tbpath/",
            "2023-01-02T00:00:00",
        )

    @patch("crisp.storage.TBPATH", "/mock/tbpath/")
    @patch("crisp.storage.DATE_TIME", "2023-01-01T00:00:00")
    @patch("crisp.storage.CRISP_SECONDARY_TBPATH", "/mock/crisp_secondary_tbpath/")
    @patch("crisp.storage.CRISP_SECONDARY_DATE_TIME", "2023-01-02T00:00:00")
    @patch("crisp.storage.Upload")
    @patch("crisp.storage.compressAndUpload")
    @patch("crisp.storage.TBPathExists")
    def test_uploadReal_skips_ri_when_path_exists(
        self,
        mock_tb_path_exists,
        mock_compress_and_upload,
        mock_upload,
    ):
        config = MagicMock()
        config.serviceName = "mock_service"
        config.operationName = "mock_operation"
        config.tracesDir = "/mock/traces"
        config.filesToUpload = ["/mock/file1"]

        # RI path already exists — RI upload should be skipped
        mock_tb_path_exists.return_value = True

        result = uploadReal(config)

        self.assertEqual(result, 0)

        # Main TBPATH upload still happens
        mock_compress_and_upload.assert_called_once()
        # Upload called only once (for TBPATH), not for CRISP_SECONDARY_TBPATH
        mock_upload.assert_called_once_with(
            config.serviceName,
            config.operationName,
            config.filesToUpload,
            "/mock/tbpath/",
            "2023-01-01T00:00:00",
        )


class TestCompressAndUpload(unittest.TestCase):
    """Test cases for compressAndUpload using mocked TBClient."""

    @patch("subprocess.check_call")
    @patch("crisp.storage.TBClient")
    @patch("crisp.storage.common.serviceOperationToTBPath")
    def test_compressAndUpload_success(self, mock_tb_path, mock_tb_client, mock_check_call):
        mock_tb_path.return_value = "/crisp/service/op/2023-01-01"
        mock_client_instance = mock_tb_client.return_value

        directory = "/tmp/test_dir"
        tgzFile = f"{directory}.tgz"

        compressAndUpload("service", "operation", directory, "/crisp/", "2023-01-01")

        mock_check_call.assert_called_once_with(
            ("tar", "-c", "--use-compress-program=pigz", "-f", tgzFile, directory)
        )
        mock_tb_path.assert_called_once_with("service", "operation", "/crisp/", "2023-01-01")
        mock_client_instance.upload_file_to_tb.assert_called_once_with(
            "/crisp/service/op/2023-01-01", tgzFile
        )

    @patch("subprocess.check_call")
    @patch("crisp.storage.TBClient")
    @patch("crisp.storage.common.serviceOperationToTBPath")
    def test_compressAndUpload_removes_tgz_on_failure(
        self, mock_tb_path, mock_tb_client, mock_check_call
    ):
        mock_check_call.side_effect = Exception("tar failed")

        directory = "/tmp/test_dir"
        tgzFile = f"{directory}.tgz"

        with patch("os.path.exists", return_value=True), patch("os.remove") as mock_remove:
            compressAndUpload("service", "operation", directory, "/crisp/", "2023-01-01")
            mock_remove.assert_called_once_with(tgzFile)


class TestUpload(unittest.TestCase):
    """Test cases for Upload using mocked TBClient."""

    @patch("crisp.storage.TBClient")
    @patch("crisp.storage.common.serviceOperationToTBPath")
    def test_Upload_calls_tb_client_per_file(self, mock_tb_path, mock_tb_client):
        mock_tb_path.return_value = "/crisp/svc/op/date"
        mock_client_instance = mock_tb_client.return_value

        files = ["file1.txt", "file2.txt"]
        Upload("svc", "op", files, "/crisp/", "date")

        self.assertEqual(mock_client_instance.upload_file_to_tb.call_count, 2)
        mock_client_instance.upload_file_to_tb.assert_any_call("/crisp/svc/op/date", "file1.txt")
        mock_client_instance.upload_file_to_tb.assert_any_call("/crisp/svc/op/date", "file2.txt")


class TestUploadCrossRegionCalls(unittest.TestCase):
    """uploadCrossRegionCalls is not implemented in the open-source build."""

    def test_raises_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            uploadCrossRegionCalls([], MagicMock())
