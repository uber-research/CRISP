import json
import unittest
from http import HTTPStatus
from unittest import mock
from unittest.mock import mock_open

from tenacity import RetryError

import crisp.common as common
import crisp.get_trace as get_trace


GET_TRACE_IDS_SUCCESS_RESPONSE = {"data": ["4efaf81da348f0f2"]}
GET_TRACE_IDS_NO_TRACES_RESPONSE = {"data": []}
DOWNLOAD_TRACES_SUCCESS_RESPONSE = b"1234"


class get_traceTestCase(unittest.TestCase):
    @mock.patch("crisp.get_trace.requests.get")
    @mock.patch("builtins.open", new_callable=mock_open)
    def test_download(self, open_mock, requests_get_mock):  # noqa: ARG002
        requests_get_mock.return_value.status_code = HTTPStatus.OK
        requests_get_mock.return_value.content = DOWNLOAD_TRACES_SUCCESS_RESPONSE

        assert HTTPStatus.OK == get_trace.download("12", common.Config(numTrace=10))

    @mock.patch("crisp.get_trace.requests.get")
    @mock.patch("builtins.open", new_callable=mock_open)
    def test_download_fail(self, open_mock, requests_get_mock):  # noqa: ARG002
        requests_get_mock.return_value.status_code = HTTPStatus.MULTIPLE_CHOICES

        assert HTTPStatus.MULTIPLE_CHOICES == get_trace.download(
            "12",
            common.Config(numTrace=10),
        )

    @mock.patch("crisp.get_trace.requests.get")
    def test_download_logs_additional_context_on_failure(self, requests_get_mock):
        response = requests_get_mock.return_value
        response.status_code = HTTPStatus.BAD_REQUEST
        response.text = "failure details"
        response.reason = "Bad Request"
        response.url = "http://jaeger/api/traces/trace-12"

        with self.assertLogs(level="WARNING") as captured:
            status = get_trace.download("trace-12", common.Config(numTrace=10))

        self.assertEqual(HTTPStatus.BAD_REQUEST, status)
        warning_lines = [line for line in captured.output if "Trace download failed" in line]
        self.assertTrue(warning_lines)
        self.assertIn("trace_id=trace-12", warning_lines[0])
        self.assertTrue(
            "status=HTTPStatus.BAD_REQUEST" in warning_lines[0] or "status=400" in warning_lines[0],
            warning_lines[0],
        )
        self.assertIn("url=http://jaeger/api/traces/trace-12", warning_lines[0])

    @mock.patch("crisp.get_trace.requests.get")
    def test_download_truncates_long_response_snippet(self, requests_get_mock):
        response = requests_get_mock.return_value
        response.status_code = HTTPStatus.BAD_REQUEST
        long_text = "x" * 250  # Over 200 character limit
        response.text = long_text
        response.reason = "Bad Request"
        response.url = "http://jaeger/api/traces/trace-12"

        with self.assertLogs(level="WARNING") as captured:
            status = get_trace.download("trace-12", common.Config(numTrace=10))

        self.assertEqual(HTTPStatus.BAD_REQUEST, status)
        warning_lines = [line for line in captured.output if "Trace download failed" in line]
        self.assertTrue(warning_lines)
        # Should contain truncated snippet with "..."
        self.assertIn("response_snippet=" + "x" * 200 + "...", warning_lines[0])

    @mock.patch("crisp.get_trace.requests.get")
    def test_download_logs_no_response(self, requests_get_mock):
        requests_get_mock.return_value = None

        with self.assertLogs(level="WARNING") as captured:
            status = get_trace.download("trace-12", common.Config(numTrace=10))

        self.assertEqual(-1, status)
        warning_lines = [line for line in captured.output if "requests.get returned no response" in line]
        self.assertTrue(warning_lines)
        self.assertIn("trace_id=trace-12", warning_lines[0])

    @mock.patch("crisp.get_trace.requests.get")
    def test_download_logs_exception(self, requests_get_mock):
        requests_get_mock.side_effect = ConnectionError("Network error")

        with self.assertLogs(level="WARNING") as captured:
            status = get_trace.download("trace-12", common.Config(numTrace=10))

        self.assertEqual(-1, status)
        warning_lines = [line for line in captured.output if "Trace download raised exception" in line]
        self.assertTrue(warning_lines)
        self.assertIn("trace_id=trace-12", warning_lines[0])
        self.assertIn("Network error", warning_lines[0])

    @mock.patch("crisp.get_trace.requests.get")
    def test_download_logs_exhausted_retries(self, requests_get_mock):
        # Mock a retryable status code that will exhaust retries
        response = requests_get_mock.return_value
        response.status_code = HTTPStatus.TOO_MANY_REQUESTS  # Retryable status
        response.text = "rate limited"
        response.reason = "Too Many Requests"
        response.url = "http://jaeger/api/traces/trace-12"

        # When retries are exhausted, tenacity will raise RetryError
        with self.assertLogs(level="DEBUG") as captured:
            with self.assertRaises(RetryError):
                get_trace.download("trace-12", common.Config(numTrace=10))

        # Should have debug logs during retries since these are retryable failures
        failure_lines = [line for line in captured.output if "Trace download failed" in line]
        self.assertTrue(failure_lines)
        # Check that the first failure contains expected info
        self.assertIn("trace_id=trace-12", failure_lines[0])
        self.assertTrue(
            "status=HTTPStatus.TOO_MANY_REQUESTS" in failure_lines[0] or "status=429" in failure_lines[0],
            failure_lines[0],
        )

    @mock.patch("crisp.get_trace.requests.get")
    def test_queryWorker(self, requests_get_mock):
        requests_get_mock.return_value.status_code = HTTPStatus.OK
        requests_get_mock.return_value.json.return_value = GET_TRACE_IDS_SUCCESS_RESPONSE

        assert 0 == get_trace.getTraceIDReal(common.Config(numTrace=10))

    @mock.patch("crisp.get_trace.shutil.disk_usage", return_value=(1e15, 2e14, 8e14))
    def test_diskCheck(self, disk_usage_mock):  # noqa: ARG002
        assert get_trace.isDiskEnough()

    @mock.patch("crisp.get_trace.shutil.disk_usage", return_value=(100, 20, 80))
    def test_diskCheckFull(self, disk_usage_mock):  # noqa: ARG002
        assert not get_trace.isDiskEnough()

    def test_statusCode(self):
        assert (
            get_trace.checkStatusCode(HTTPStatus.TOO_MANY_REQUESTS)
            and get_trace.checkStatusCode(HTTPStatus.REQUEST_TIMEOUT)
            and get_trace.checkStatusCode(HTTPStatus.GATEWAY_TIMEOUT)
        )

    @mock.patch("crisp.get_trace.requests.get")
    def test_getTraceIDRealSuccessOnFirstAttempt(self, requests_get_mock):
        requests_get_mock.return_value.status_code = HTTPStatus.OK
        requests_get_mock.return_value.json.return_value = GET_TRACE_IDS_SUCCESS_RESPONSE

        c = common.Config(serviceName="S1", operationName="O1", numTrace=25)
        get_trace.getTraceIDReal(c)
        requests_get_mock.assert_called_once()
        assert 1 == len(c.traceIDs)

    @mock.patch("crisp.get_trace.requests.get")
    def test_getTraceIDRealSuccessOnSecondAttempt(self, requests_get_mock):
        trace_ids_first_attempt_response = mock.Mock()
        trace_ids_first_attempt_response.json.return_value = GET_TRACE_IDS_NO_TRACES_RESPONSE
        trace_ids_first_attempt_response.status_code = HTTPStatus.BAD_REQUEST

        trace_ids_second_attempt_response = mock.Mock()
        trace_ids_second_attempt_response.json.return_value = GET_TRACE_IDS_SUCCESS_RESPONSE
        trace_ids_second_attempt_response.status_code = HTTPStatus.OK

        requests_get_mock.side_effect = [trace_ids_first_attempt_response, trace_ids_second_attempt_response]

        c = common.Config(serviceName="S1", operationName="O1", numTrace=25)
        get_trace.getTraceIDReal(c)
        assert requests_get_mock.call_count == 2
        assert 1 == len(c.traceIDs)

    @mock.patch("crisp.get_trace.requests.get")
    def test_getTraceIDRealAlwaysFailure(self, requests_get_mock):
        trace_ids_first_attempt_response = mock.Mock()
        trace_ids_first_attempt_response.json.return_value = GET_TRACE_IDS_NO_TRACES_RESPONSE
        trace_ids_first_attempt_response.status_code = HTTPStatus.BAD_REQUEST

        trace_ids_second_attempt_response = mock.Mock()
        trace_ids_second_attempt_response.json.return_value = GET_TRACE_IDS_NO_TRACES_RESPONSE
        trace_ids_second_attempt_response.status_code = HTTPStatus.BAD_REQUEST

        requests_get_mock.side_effect = [trace_ids_first_attempt_response, trace_ids_second_attempt_response]

        c = common.Config(serviceName="S1", operationName="O1", numTrace=25)
        from crisp.exceptions import NoTraceIDsFoundException
        with self.assertRaises(NoTraceIDsFoundException):
            get_trace.getTraceIDReal(c)
        assert requests_get_mock.call_count == 2

    @mock.patch("crisp.get_trace.requests.get")
    def test_getTraceIDs_dynamic_tags_with_error(self, requests_get_mock):
        config = common.Config(serviceName="TestService", operationName="TestOperation", numTrace=5)
        config.dryRun = False
        config.errorAnalysis = True
        dummy_response = mock.Mock()
        dummy_response.status_code = HTTPStatus.OK
        dummy_response.json.return_value = {"data": ["trace1"]}
        requests_get_mock.return_value = dummy_response

        trace_ids = get_trace.getTraceIDs(1000, 2000, config, flags=2)

        # Verify that the 'tags' parameter in the GET call includes dynamic tags
        args, kwargs = requests_get_mock.call_args
        params = kwargs.get("params")
        self.assertIsNotNone(params)
        self.assertIn("tags", params)
        tags = json.loads(params["tags"])
        expected_tags = {"jaeger.flags": "2", "error": "true"}
        self.assertEqual(tags, expected_tags)
        self.assertEqual(trace_ids, ["trace1"])

    @mock.patch("crisp.get_trace.requests.get")
    def test_getTraceIDs_dynamic_tags_without_error(self, requests_get_mock):
        config = common.Config(serviceName="TestService", operationName="TestOperation", numTrace=5)
        config.dryRun = False
        config.errorAnalysis = False
        dummy_response = mock.Mock()
        dummy_response.status_code = HTTPStatus.OK
        dummy_response.json.return_value = {"data": ["trace2"]}
        requests_get_mock.return_value = dummy_response

        trace_ids = get_trace.getTraceIDs(1000, 2000, config, flags=3)

        args, kwargs = requests_get_mock.call_args
        params = kwargs.get("params")
        self.assertIsNotNone(params)
        self.assertIn("tags", params)
        tags = json.loads(params["tags"])
        expected_tags = {"jaeger.flags": "3"}
        self.assertEqual(tags, expected_tags)
        self.assertEqual(trace_ids, ["trace2"])

    def test_getTraceIDs_dry_run(self):
        # When dryRun is True, getTraceIDs should return an empty list without calling requests.get
        config = common.Config(serviceName="TestService", operationName="TestOperation", numTrace=5)
        config.dryRun = True
        trace_ids = get_trace.getTraceIDs(1000, 2000, config)
        self.assertEqual(trace_ids, [])
