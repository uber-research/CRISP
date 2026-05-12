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
    def setUp(self) -> None:
        reset_config()

    def tearDown(self) -> None:
        reset_config()

    def test_default_overlap_allowance_fraction(self) -> None:
        assert AnalysisConfig().overlap_allowance_fraction == 0.01

    def test_default_server_lengthening_factor(self) -> None:
        assert AnalysisConfig().server_lengthening_factor == 1.01

    def test_default_flags_false(self) -> None:
        cfg = AnalysisConfig()
        assert cfg.enable_optimistic_time_saved is False
        assert cfg.enable_pessimistic_time_saved is False

    def test_default_parallelism(self) -> None:
        cfg = AnalysisConfig()
        assert cfg.default_compute_parallelism == 16
        assert cfg.default_io_parallelism == 4

    def test_default_downloads(self) -> None:
        assert AnalysisConfig().max_concurrent_downloads == 5

    def test_default_retry_config(self) -> None:
        cfg = AnalysisConfig()
        assert cfg.max_retry_attempts == 3
        assert cfg.retry_min_wait_seconds == 1
        assert cfg.retry_max_wait_seconds == 10
        assert cfg.retry_jitter_max_seconds == 2


class TestGetConfig(unittest.TestCase):
    def setUp(self) -> None:
        reset_config()

    def tearDown(self) -> None:
        reset_config()

    def test_returns_analysis_config(self) -> None:
        assert isinstance(get_config(), AnalysisConfig)

    def test_singleton_same_object(self) -> None:
        # repeated calls return the same instance
        assert get_config() is get_config()

    def test_set_config_replaces_singleton(self) -> None:
        custom = AnalysisConfig(overlap_allowance_fraction=0.5)
        set_config(custom)
        assert get_config() is custom
        assert get_config().overlap_allowance_fraction == 0.5

    def test_reset_config_restores_defaults(self) -> None:
        set_config(AnalysisConfig(overlap_allowance_fraction=0.99))
        reset_config()
        assert get_config().overlap_allowance_fraction == 0.01

    def test_reset_config_returns_new_instance(self) -> None:
        first = get_config()
        reset_config()
        assert get_config() is not first


class TestConfigBuilder(unittest.TestCase):
    def setUp(self) -> None:
        reset_config()

    def tearDown(self) -> None:
        reset_config()

    def test_build_returns_analysis_config(self) -> None:
        cfg = ConfigBuilder().build()
        assert isinstance(cfg, AnalysisConfig)

    def test_overlap_allowance(self) -> None:
        cfg = ConfigBuilder().overlap_allowance(0.05).build()
        assert cfg.overlap_allowance_fraction == 0.05

    def test_server_lengthening(self) -> None:
        cfg = ConfigBuilder().server_lengthening(1.1).build()
        assert cfg.server_lengthening_factor == 1.1

    def test_enable_optimistic(self) -> None:
        cfg = ConfigBuilder().enable_optimistic().build()
        assert cfg.enable_optimistic_time_saved is True

    def test_enable_optimistic_explicit_false(self) -> None:
        cfg = ConfigBuilder().enable_optimistic(False).build()
        assert cfg.enable_optimistic_time_saved is False

    def test_enable_pessimistic(self) -> None:
        cfg = ConfigBuilder().enable_pessimistic().build()
        assert cfg.enable_pessimistic_time_saved is True

    def test_parallelism(self) -> None:
        cfg = ConfigBuilder().parallelism(compute=8, io=2).build()
        assert cfg.default_compute_parallelism == 8
        assert cfg.default_io_parallelism == 2

    def test_retry_config(self) -> None:
        cfg = ConfigBuilder().retry_config(max_attempts=5, min_wait=2, max_wait=20, jitter=3).build()
        assert cfg.max_retry_attempts == 5
        assert cfg.retry_min_wait_seconds == 2
        assert cfg.retry_max_wait_seconds == 20
        assert cfg.retry_jitter_max_seconds == 3

    def test_fluent_chaining(self) -> None:
        cfg = (
            ConfigBuilder()
            .overlap_allowance(0.02)
            .server_lengthening(1.05)
            .enable_optimistic()
            .parallelism(4, 1)
            .build()
        )
        assert cfg.overlap_allowance_fraction == 0.02
        assert cfg.server_lengthening_factor == 1.05
        assert cfg.enable_optimistic_time_saved is True
        assert cfg.default_compute_parallelism == 4
        assert cfg.default_io_parallelism == 1


class TestConvenienceFunctions(unittest.TestCase):
    def setUp(self) -> None:
        reset_config()

    def tearDown(self) -> None:
        reset_config()

    def test_get_overlap_allowance_default(self) -> None:
        assert get_overlap_allowance() == 0.01

    def test_get_overlap_allowance_custom(self) -> None:
        set_config(AnalysisConfig(overlap_allowance_fraction=0.07))
        assert get_overlap_allowance() == 0.07

    def test_get_server_lengthening_factor_default(self) -> None:
        assert get_server_lengthening_factor() == 1.01

    def test_get_server_lengthening_factor_custom(self) -> None:
        set_config(AnalysisConfig(server_lengthening_factor=1.5))
        assert get_server_lengthening_factor() == 1.5

    def test_is_optimistic_enabled_default(self) -> None:
        assert is_optimistic_enabled() is False

    def test_is_optimistic_enabled_true(self) -> None:
        set_config(AnalysisConfig(enable_optimistic_time_saved=True))
        assert is_optimistic_enabled() is True

    def test_is_pessimistic_enabled_default(self) -> None:
        assert is_pessimistic_enabled() is False

    def test_is_pessimistic_enabled_true(self) -> None:
        set_config(AnalysisConfig(enable_pessimistic_time_saved=True))
        assert is_pessimistic_enabled() is True
