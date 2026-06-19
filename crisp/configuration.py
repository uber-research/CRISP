"""Configuration settings for critical path analysis.

This module contains configurable parameters and settings that control
the behavior of the critical path analysis algorithms.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AnalysisConfig:
    """Configuration class for critical path analysis parameters.

    This class contains all the configurable settings that affect
    how the critical path analysis algorithms behave.
    """

    # Overlap and timing tolerances
    overlap_allowance_fraction: float = 0.01
    """How much spans can overlap as a fraction of the total execution time of their common parent."""

    server_lengthening_factor: float = 1.01
    """How much a server span can be greater than its client span."""

    # Algorithm control flags
    enable_optimistic_time_saved: bool = False
    """Enable optimistic time saved calculations (kept for reference, not used)."""

    enable_pessimistic_time_saved: bool = False
    """Enable pessimistic time saved calculations (kept for reference, not used)."""

    # Processing limits and defaults
    max_concurrent_downloads: int = 5
    """Maximum number of concurrent trace downloads allowed."""

    default_compute_parallelism: int = 16
    """Default number of parallel compute operations."""

    default_io_parallelism: int = 4
    """Default number of parallel I/O operations."""

    # Retry configuration
    max_retry_attempts: int = 3
    """Maximum number of retry attempts for failed operations."""

    retry_min_wait_seconds: int = 1
    """Minimum wait time between retries in seconds."""

    retry_max_wait_seconds: int = 10
    """Maximum wait time between retries in seconds."""

    retry_jitter_max_seconds: int = 2
    """Maximum jitter to add to retry wait times in seconds."""


# Global configuration instance
_config: Optional[AnalysisConfig] = None


def get_config() -> AnalysisConfig:
    """Get the global analysis configuration instance.

    Returns:
        AnalysisConfig: The current configuration instance.
    """
    global _config
    if _config is None:
        _config = AnalysisConfig()
    return _config


def set_config(config: AnalysisConfig) -> None:
    """Set the global analysis configuration instance.

    Args:
        config: The new configuration to use.
    """
    global _config
    _config = config


def reset_config() -> None:
    """Reset the configuration to default values."""
    global _config
    _config = AnalysisConfig()


class ConfigBuilder:
    """Builder class for creating custom analysis configurations."""

    def __init__(self) -> None:
        self._config = AnalysisConfig()

    def overlap_allowance(self, fraction: float) -> 'ConfigBuilder':
        """Set the overlap allowance fraction."""
        self._config.overlap_allowance_fraction = fraction
        return self

    def server_lengthening(self, factor: float) -> 'ConfigBuilder':
        """Set the server lengthening factor."""
        self._config.server_lengthening_factor = factor
        return self

    def enable_optimistic(self, enabled: bool = True) -> 'ConfigBuilder':
        """Enable or disable optimistic time saved calculations."""
        self._config.enable_optimistic_time_saved = enabled
        return self

    def enable_pessimistic(self, enabled: bool = True) -> 'ConfigBuilder':
        """Enable or disable pessimistic time saved calculations."""
        self._config.enable_pessimistic_time_saved = enabled
        return self

    def parallelism(self, compute: int, io: int) -> 'ConfigBuilder':
        """Set compute and I/O parallelism levels."""
        self._config.default_compute_parallelism = compute
        self._config.default_io_parallelism = io
        return self

    def retry_config(self, max_attempts: int, min_wait: int, max_wait: int, jitter: int) -> 'ConfigBuilder':
        """Set retry configuration parameters."""
        self._config.max_retry_attempts = max_attempts
        self._config.retry_min_wait_seconds = min_wait
        self._config.retry_max_wait_seconds = max_wait
        self._config.retry_jitter_max_seconds = jitter
        return self

    def build(self) -> AnalysisConfig:
        """Build and return the configuration."""
        return self._config


# Convenience functions for commonly used config values
def get_overlap_allowance() -> float:
    """Get the current overlap allowance fraction."""
    return get_config().overlap_allowance_fraction


def get_server_lengthening_factor() -> float:
    """Get the current server lengthening factor."""
    return get_config().server_lengthening_factor


def is_optimistic_enabled() -> bool:
    """Check if optimistic time saved calculations are enabled."""
    return get_config().enable_optimistic_time_saved


def is_pessimistic_enabled() -> bool:
    """Check if pessimistic time saved calculations are enabled."""
    return get_config().enable_pessimistic_time_saved
