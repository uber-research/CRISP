import unittest

from crisp.configuration import (
    AnalysisConfig,
    ConfigBuilder,
    get_config,
    get_overlap_allowance,
    get_server_lengthening_factor,
    is_optimistic_enabled,
    is_pessimistic_enabled,
    reset_config,
    set_config,
)


class TestAnalysisConfigDefaults(unittest.TestCase):
    def setUp(self):
        reset_config()

    def tearDown(self):
        reset_config()

    def test_overlap_allowance_fraction_default(self):
        cfg = AnalysisConfig()
        self.assertAlmostEqual(cfg.overlap_allowance_fraction, 0.01)

    def test_server_lengthening_factor_default(self):
        cfg = AnalysisConfig()
        self.assertAlmostEqual(cfg.server_lengthening_factor, 1.01)

    def test_enable_optimistic_time_saved_default(self):
        cfg = AnalysisConfig()
        self.assertFalse(cfg.enable_optimistic_time_saved)

    def test_enable_pessimistic_time_saved_default(self):
        cfg = AnalysisConfig()
        self.assertFalse(cfg.enable_pessimistic_time_saved)

    def test_max_concurrent_downloads_default(self):
        cfg = AnalysisConfig()
        self.assertEqual(cfg.max_concurrent_downloads, 5)

    def test_default_compute_parallelism_default(self):
        cfg = AnalysisConfig()
        self.assertEqual(cfg.default_compute_parallelism, 16)

    def test_default_io_parallelism_default(self):
        cfg = AnalysisConfig()
        self.assertEqual(cfg.default_io_parallelism, 4)

    def test_max_retry_attempts_default(self):
        cfg = AnalysisConfig()
        self.assertEqual(cfg.max_retry_attempts, 3)

    def test_retry_min_wait_seconds_default(self):
        cfg = AnalysisConfig()
        self.assertEqual(cfg.retry_min_wait_seconds, 1)

    def test_retry_max_wait_seconds_default(self):
        cfg = AnalysisConfig()
        self.assertEqual(cfg.retry_max_wait_seconds, 10)

    def test_retry_jitter_max_seconds_default(self):
        cfg = AnalysisConfig()
        self.assertEqual(cfg.retry_jitter_max_seconds, 2)


class TestGlobalConfigSingleton(unittest.TestCase):
    def setUp(self):
        reset_config()

    def tearDown(self):
        reset_config()

    def test_get_config_returns_instance(self):
        cfg = get_config()
        self.assertIsInstance(cfg, AnalysisConfig)

    def test_get_config_returns_same_object(self):
        self.assertIs(get_config(), get_config())

    def test_set_config_replaces_singleton(self):
        new_cfg = AnalysisConfig(overlap_allowance_fraction=0.05)
        set_config(new_cfg)
        self.assertIs(get_config(), new_cfg)
        self.assertAlmostEqual(get_config().overlap_allowance_fraction, 0.05)

    def test_reset_config_restores_defaults(self):
        set_config(AnalysisConfig(overlap_allowance_fraction=0.99))
        reset_config()
        self.assertAlmostEqual(get_config().overlap_allowance_fraction, 0.01)

    def test_reset_config_creates_new_object(self):
        original = get_config()
        reset_config()
        self.assertIsNot(get_config(), original)


class TestConfigBuilder(unittest.TestCase):
    def setUp(self):
        reset_config()

    def tearDown(self):
        reset_config()

    def test_build_returns_analysis_config(self):
        cfg = ConfigBuilder().build()
        self.assertIsInstance(cfg, AnalysisConfig)

    def test_overlap_allowance(self):
        cfg = ConfigBuilder().overlap_allowance(0.05).build()
        self.assertAlmostEqual(cfg.overlap_allowance_fraction, 0.05)

    def test_server_lengthening(self):
        cfg = ConfigBuilder().server_lengthening(1.10).build()
        self.assertAlmostEqual(cfg.server_lengthening_factor, 1.10)

    def test_enable_optimistic(self):
        cfg = ConfigBuilder().enable_optimistic().build()
        self.assertTrue(cfg.enable_optimistic_time_saved)

    def test_enable_optimistic_false(self):
        cfg = ConfigBuilder().enable_optimistic(False).build()
        self.assertFalse(cfg.enable_optimistic_time_saved)

    def test_enable_pessimistic(self):
        cfg = ConfigBuilder().enable_pessimistic().build()
        self.assertTrue(cfg.enable_pessimistic_time_saved)

    def test_parallelism(self):
        cfg = ConfigBuilder().parallelism(compute=8, io=2).build()
        self.assertEqual(cfg.default_compute_parallelism, 8)
        self.assertEqual(cfg.default_io_parallelism, 2)

    def test_retry_config(self):
        cfg = ConfigBuilder().retry_config(max_attempts=5, min_wait=2, max_wait=20, jitter=3).build()
        self.assertEqual(cfg.max_retry_attempts, 5)
        self.assertEqual(cfg.retry_min_wait_seconds, 2)
        self.assertEqual(cfg.retry_max_wait_seconds, 20)
        self.assertEqual(cfg.retry_jitter_max_seconds, 3)

    def test_chaining(self):
        cfg = (
            ConfigBuilder()
            .overlap_allowance(0.02)
            .server_lengthening(1.05)
            .enable_optimistic()
            .parallelism(4, 1)
            .build()
        )
        self.assertAlmostEqual(cfg.overlap_allowance_fraction, 0.02)
        self.assertAlmostEqual(cfg.server_lengthening_factor, 1.05)
        self.assertTrue(cfg.enable_optimistic_time_saved)
        self.assertEqual(cfg.default_compute_parallelism, 4)


class TestConvenienceFunctions(unittest.TestCase):
    def setUp(self):
        reset_config()

    def tearDown(self):
        reset_config()

    def test_get_overlap_allowance_default(self):
        self.assertAlmostEqual(get_overlap_allowance(), 0.01)

    def test_get_overlap_allowance_after_set(self):
        set_config(AnalysisConfig(overlap_allowance_fraction=0.07))
        self.assertAlmostEqual(get_overlap_allowance(), 0.07)

    def test_get_server_lengthening_factor_default(self):
        self.assertAlmostEqual(get_server_lengthening_factor(), 1.01)

    def test_get_server_lengthening_factor_after_set(self):
        set_config(AnalysisConfig(server_lengthening_factor=1.20))
        self.assertAlmostEqual(get_server_lengthening_factor(), 1.20)

    def test_is_optimistic_enabled_default(self):
        self.assertFalse(is_optimistic_enabled())

    def test_is_optimistic_enabled_after_set(self):
        set_config(AnalysisConfig(enable_optimistic_time_saved=True))
        self.assertTrue(is_optimistic_enabled())

    def test_is_pessimistic_enabled_default(self):
        self.assertFalse(is_pessimistic_enabled())

    def test_is_pessimistic_enabled_after_set(self):
        set_config(AnalysisConfig(enable_pessimistic_time_saved=True))
        self.assertTrue(is_pessimistic_enabled())
