import time
from unittest import TestCase, mock

import crisp.common as common


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


class TbutilTestCase(TestCase):
    def test_replaceNonAlphaNumericWithUnderscore(self):
        res = common.replaceNonAlphaNumericWithUnderscore("Bloc::renderTemplate")
        assert res == "Bloc_renderTemplate"
        res = common.replaceNonAlphaNumericWithUnderscore("my-service-group1")
        assert res == "my_service_group1"
        res = common.replaceNonAlphaNumericWithUnderscore("/api/:resourceId/v2/action")
        assert res == "_api_resourceId_v2_action"
        res = common.replaceNonAlphaNumericWithUnderscore("my-service.my-operation")
        assert res == "my_service_my_operation"


class GetMidnightTimestampTestCase(TestCase):
    def test_getMidnightTimeStamp(self):
        assert time.time() * 1000 * 1000 >= common.getMidnightTimeStamp()


class TestIntToHexString(TestCase):
    def test_positive_value(self):
        result = common.intToHexString(1234567890)
        self.assertEqual(result, '00000000499602d2')

    def test_negative_value(self):
        result = common.intToHexString(-1234567890)
        self.assertEqual(result, 'ffffffffb669fd2e')

    def test_zero_value(self):
        result = common.intToHexString(0)
        self.assertEqual(result, '0000000000000000')

    def test_max_positive_value(self):
        result = common.intToHexString(9223372036854775807)
        self.assertEqual(result, '7fffffffffffffff')

    def test_max_negative_value(self):
        result = common.intToHexString(-9223372036854775808)
        self.assertEqual(result, '8000000000000000')


class TestConfig(TestCase):
    def test_default_field_values(self):
        c = common.Config()
        self.assertEqual(c.operationName, "")
        self.assertEqual(c.serviceName, "")
        self.assertEqual(c.numTrace, 1000)
        self.assertEqual(c.ioParallelism, 1)
        self.assertEqual(c.computeParallelism, 1)
        self.assertEqual(c.lookbackDays, 1)
        self.assertEqual(c.topN, 5)
        self.assertEqual(c.numHMTrace, 100)
        self.assertEqual(c.numOperation, 100)
        self.assertEqual(c.diskRequirement, 5)
        self.assertFalse(c.doRanges)
        self.assertFalse(c.dryRun)
        self.assertTrue(c.mergeAllRoots)
        self.assertEqual(c.maxExemplars, 3)

    def test_callsPerWorker_default(self):
        c = common.Config()
        # max(1, int(500 / (1 * 1))) == 500
        self.assertEqual(c.callsPerWorker, 500)

    def test_callsPerWorker_custom(self):
        c = common.Config(qps=100, ioParallelism=2, numShards=5)
        # max(1, int(100 / (2 * 5))) == max(1, 10) == 10
        self.assertEqual(c.callsPerWorker, 10)

    def test_projectionEnabled_false_when_none(self):
        c = common.Config()
        self.assertFalse(c.projectionEnabled)

    def test_projectionEnabled_false_when_only_service(self):
        c = common.Config(deltaTargetService="svc")
        self.assertFalse(c.projectionEnabled)

    def test_projectionEnabled_true_when_both_set(self):
        c = common.Config(deltaTargetService="svc", deltaTargetOperation="op")
        self.assertTrue(c.projectionEnabled)

    def test_getOutputDir_returns_tracesDir_when_file_none(self):
        c = common.Config(tracesDir="my_traces")
        self.assertEqual(c.getOutputDir(), "my_traces")

    def test_getOutputDir_returns_dirname_when_file_set(self):
        fake_file = mock.MagicMock()
        fake_file.name = "/some/dir/traces.json"
        c = common.Config(file=fake_file)
        self.assertEqual(c.getOutputDir(), "/some/dir")

    def test_timestamps_auto_computed(self):
        c = common.Config()
        self.assertIsInstance(c.startTimestamp, int)
        self.assertIsInstance(c.endTimestamp, int)
        self.assertGreater(c.startTimestamp, 0)
        self.assertGreater(c.endTimestamp, 0)

    def test_timestamps_explicit(self):
        c = common.Config(startTimestamp=1000, endTimestamp=2000)
        self.assertEqual(c.startTimestamp, 1000)
        self.assertEqual(c.endTimestamp, 2000)

    def test_removed_fields_not_present(self):
        c = common.Config()
        self.assertFalse(hasattr(c, 'useUSSO'))
        self.assertFalse(hasattr(c, 'uploadToTB'))
        self.assertFalse(hasattr(c, 'uploadToCrispRiTB'))
        self.assertFalse(hasattr(c, 'uploadTar'))
        self.assertFalse(hasattr(c, 'noOverwriteUpload'))
        self.assertFalse(hasattr(c, 'ignoreCtfTests'))
        self.assertFalse(hasattr(c, 'enableM3Metrics'))
        self.assertFalse(hasattr(c, 'jobTag'))
        self.assertFalse(hasattr(c, 'useParquet'))
        self.assertFalse(hasattr(c, 'jaegerOfflineToken'))
        self.assertFalse(hasattr(c, 'terrablobOfflineToken'))
        self.assertFalse(hasattr(c, 'serviceMode'))
        self.assertFalse(hasattr(c, 'zone'))

    def test_unconditional_instance_vars(self):
        c = common.Config()
        self.assertEqual(c.jaegerTraceFiles, [])
        self.assertIsNone(c.filesToUpload)
        self.assertEqual(c.traceIDs, [])
        self.assertFalse(c.failed)
        self.assertEqual(c.failedLog, [])


class TestServiceOperationToTBPath(TestCase):
    def test_basic(self):
        result = common.serviceOperationToTBPath(
            "my-service", "MyService::doThing", "/output", "2024_01_01"
        )
        self.assertEqual(result, "/output/my_service/MyService_doThing/2024_01_01")

    def test_latest_suffix(self):
        result = common.serviceOperationToTBPath(
            "my-service", "MyService::doThing", "/output", "latest"
        )
        self.assertEqual(result, "/output/my_service/MyService_doThing/latest")


class TestGetServiceOperationTags(TestCase):
    def test_returns_correct_keys(self):
        c = common.Config(serviceName="my-service", operationName="MyService::doThing")
        tags = common.getServiceOperationTags(c)
        self.assertEqual(tags[common.SERVICE_TAG_NAME], "my_service")
        self.assertEqual(tags[common.OPERATION_TAG_NAME], "MyService_doThing")

    def test_default_config(self):
        c = common.Config()
        tags = common.getServiceOperationTags(c)
        self.assertIn(common.SERVICE_TAG_NAME, tags)
        self.assertIn(common.OPERATION_TAG_NAME, tags)
