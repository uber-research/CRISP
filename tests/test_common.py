import time
import os
import sys
from unittest import TestCase, mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import common as common


class CommonTestCase(TestCase):
    def test_templateHandler(self):
        def realHandler(c):
            pass

        def preStart(c):
            pass

        def postFinish(c):
            pass

        c = common.Config()
        resultQ = mock.MagicMock()
        common.templateHandler("message", realHandler, preStart, postFinish, c, resultQ)
        resultQ.put.assert_called_once_with(c)
        resultQ.close.assert_called_once()


# Test cases for MetricVals class
class TestMetricVals(TestCase):
    def test_addition(self):
        metric1 = common.MetricVals(1, 2, 3, 100)
        metric2 = common.MetricVals(4, 5, 6, 200)
        result = metric1 + metric2
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 7)
        self.assertEqual(result.freq, 9)

    def test_in_place_addition(self):
        metric = common.MetricVals(1, 2, 3, 100)
        metric += common.MetricVals(4, 5, 6, 200)
        self.assertEqual(metric.inc, 5)
        self.assertEqual(metric.excl, 7)
        self.assertEqual(metric.freq, 9)

    def test_floordiv(self):
        metric = common.MetricVals(10, 20, 30, 100)
        result = metric // 2
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 10)
        self.assertEqual(result.freq, 15)

    def test_in_place_floordiv(self):
        metric = common.MetricVals(10, 20, 30, 100)
        metric //= 2
        self.assertEqual(metric.inc, 5)
        self.assertEqual(metric.excl, 10)
        self.assertEqual(metric.freq, 15)


# Test cases for CallPathProfile class
class TestCallPathProfile(TestCase):
    def setUp(self):
        self.metric1 = common.MetricVals(1, 2, 3, 100)
        self.metric2 = common.MetricVals(4, 5, 6, 200)
        self.profile1 = common.CallPathProfile({"path1": self.metric1}, 2, 1)
        self.profile2 = common.CallPathProfile({"path2": self.metric2}, 3, 2)

    def test_get_normalized(self):
        result = self.profile1.GetNormalized()
        self.assertEqual(result["path1"].inc, 0)
        self.assertEqual(result["path1"].excl, 1)
        self.assertEqual(result["path1"].freq, 1)

    def test_normalize(self):
        self.profile1.Normalize()
        self.assertEqual(self.profile1.profile["path1"].inc, 0)
        self.assertEqual(self.profile1.profile["path1"].excl, 1)
        self.assertEqual(self.profile1.profile["path1"].freq, 1)

    def test_normalize_field(self):
        self.profile1.NormalizeField("inc")
        self.assertEqual(self.profile1.profile["path1"].inc, 0)

    def test_upsert_existing(self):
        self.profile1.Upsert("path1", common.MetricVals(1, 1, 1, 300))
        self.assertEqual(self.profile1.profile["path1"].inc, 2)
        self.assertEqual(self.profile1.profile["path1"].excl, 3)
        self.assertEqual(self.profile1.profile["path1"].freq, 4)

    def test_upsert_new(self):
        self.profile1.Upsert("path3", common.MetricVals(1, 1, 1, 300))
        self.assertEqual(self.profile1.profile["path3"].inc, 1)
        self.assertEqual(self.profile1.profile["path3"].excl, 1)
        self.assertEqual(self.profile1.profile["path3"].freq, 1)

    def test_add_profiles(self):
        result = self.profile1 + self.profile2
        self.assertIn("path1", result.profile)
        self.assertIn("path2", result.profile)
        self.assertEqual(result.count, 5)

    def test_in_place_add_profiles(self):
        self.profile1 += self.profile2
        self.assertIn("path1", self.profile1.profile)
        self.assertIn("path2", self.profile1.profile)
        self.assertEqual(self.profile1.count, 5)


class TestGetLeafNodeFromCallPath(TestCase):
    def test_regular_path(self):
        path = "node1->node2->node3"
        result = common.getLeafNodeFromCallPath(path)
        self.assertEqual(result, "node3")

    def test_single_node_path(self):
        path = "singleNode"
        result = common.getLeafNodeFromCallPath(path)
        self.assertEqual(result, "singleNode")

    def test_empty_string(self):
        path = ""
        result = common.getLeafNodeFromCallPath(path)
        self.assertEqual(result, "")


class TbutilTestCase(TestCase):
    def test_replaceNonAlphaNumericWithUnderscore(self):
        res = common.replaceNonAlphaNumericWithUnderscore("Service::OperationName")
        self.assertEqual(res, "Service_OperationName")
        res = common.replaceNonAlphaNumericWithUnderscore("api-group1-service")
        self.assertEqual(res, "api_group1_service")
        res = common.replaceNonAlphaNumericWithUnderscore(
            "/users/:userID/v1/action",
        )
        self.assertEqual(res, "_users_userID_v1_action")
        res = common.replaceNonAlphaNumericWithUnderscore(
            "presentation.handler.process-task",
        )
        self.assertEqual(res, "presentation_handler_process_task")

    def test_downloadFromTerrablob(self):
        with mock.patch("common.TBClient") as tb_client_mock:
            tb_client_obj = tb_client_mock.return_value
            tb_client_obj.download_file_from_tb.return_value = "temp/folder/file.csv"
            res = common.downloadFromTerrablob("remote_file.csv", "temp/folder/file.csv")
            self.assertEqual(res, "temp/folder/file.csv")
            tb_client_obj.download_file_from_tb.assert_called_once_with(
                tb_file_path="remote_file.csv",
                local_file_path="temp/folder/file.csv",
            )

    @mock.patch("common.TBClient")
    def test_uploadToTerrablob(self, tb_client_mock):
        tb_client_obj = tb_client_mock.return_value
        tb_client_obj.upload_file_to_tb.return_value = "/example/path/data/2023_01_01/file.csv"

        res = common.uploadToTerrablob(
            "local/folder/file.csv",
            "/example/path/data/2023_01_01",
        )

        self.assertEqual(res, "/example/path/data/2023_01_01/file.csv")
        tb_client_obj.upload_file_to_tb.assert_called_once_with(
            tb_file_path="/example/path/data/2023_01_01/file.csv",
            local_file_path="local/folder/file.csv",
        )

    @mock.patch("common.TBClient")
    def test_constructPathAndUploadToTerrablob(self, tb_client_mock):
        tb_client_obj = tb_client_mock.return_value
        tb_client_obj.upload_file_to_tb.side_effect = [
            "/example/path/data/service/Service_renderMetrics/2023_01_01/file.csv",
            "/example/path/data/service/Service_renderMetrics/latest/file.csv",
            "/example/alternative_path/data/service/Service_renderMetrics/2023_01_01/file.csv",
        ]

        tbPath, latestTbPath = common.constructPathAndUploadToTerrablob(
            "/example/path/data",
            "service",
            "Service::renderMetrics",
            "local/folder/file.csv",
            "2023_01_01",
            publishAsLatest=True,
        )

        self.assertEqual(tbPath, "/example/path/data/service/Service_renderMetrics/2023_01_01/file.csv")
        self.assertEqual(latestTbPath, "/example/path/data/service/Service_renderMetrics/latest/file.csv")

        tbPath, latestTbPath = common.constructPathAndUploadToTerrablob(
            "/example/alternative_path/data",
            "service",
            "Service::renderMetrics",
            "local/folder/file.csv",
            "2023_01_01",
            publishAsLatest=False,
        )
        self.assertEqual(tbPath, "/example/alternative_path/data/service/Service_renderMetrics/2023_01_01/file.csv")
        self.assertIsNone(latestTbPath)


class GetMidnightTimestampTestCase(TestCase):
    def test_getMidnightTimeStamp(self):
        assert time.time() * 1000 * 1000 >= common.getMidnightTimeStamp()

class TestIntToHexString(TestCase):

    def test_positive_value(self):
        # Test a positive integer
        value = 1234567890
        expected_hex = '00000000499602d2'  # Expected hex string for the positive value
        result = common.intToHexString(value)
        self.assertEqual(result, expected_hex)

    def test_negative_value(self):
        # Test a negative integer
        value = -1234567890
        expected_hex = 'ffffffffb669fd2e'  # Expected hex string for the negative value
        result = common.intToHexString(value)
        self.assertEqual(result, expected_hex)

    def test_zero_value(self):
        # Test zero
        value = 0
        expected_hex = '0000000000000000'  # Expected hex string for zero
        result = common.intToHexString(value)
        self.assertEqual(result, expected_hex)

    def test_max_positive_value(self):
        # Test the maximum positive 64-bit signed integer
        value = 9223372036854775807
        expected_hex = '7fffffffffffffff'  # Expected hex string for the maximum positive value
        result = common.intToHexString(value)
        self.assertEqual(result, expected_hex)

    def test_max_negative_value(self):
        # Test the maximum negative 64-bit signed integer
        value = -9223372036854775808
        expected_hex = '8000000000000000'  # Expected hex string for the maximum negative value
        result = common.intToHexString(value)
        self.assertEqual(result, expected_hex)
