"""Constants used across the critical path analysis modules."""

# Base URL of the Jaeger UI that trace links point at. Defaults to the
# upstream Jaeger all-in-one default (port 16686) so
# ``docker run jaegertracing/all-in-one`` works out of the box. Override
# this with your deployment's URL — see crisp/configuration.py once that
# lands for a config-driven approach. The trailing slash matters:
# call sites concatenate directly as f"{JAEGER_UI_URL}{traceId}?...".
JAEGER_UI_URL = "http://localhost:16686/trace/"

# HTML / FontAwesome class appended to sortable table headers in rendered
# reports.
SORTABLE_COL_CLASS = ' <i class="fas fa-sort"></i>'

# Metric field name referenced by several CSV/output generators.
TOTAL_TIME = "totalTime"

# Sentinel values used by QuantizedMetrics.__init__ to compute the
# min/max depth of a histogram via running min/max against these
# intentionally-extreme seeds.
DEFAULT_MIN_DEPTH = 1000000000
DEFAULT_MAX_DEPTH = -1

# Percentiles (as fractions in [0, 1]) used by QuantizedMetrics and
# related distribution summaries.
PERCENTILE_50 = 0.5
PERCENTILE_90 = 0.9
PERCENTILE_95 = 0.95
PERCENTILE_99 = 0.99

# Zero-initializers for the three error-count categories tracked by
# ErrCountsData.
DEFAULT_SELF_ERRORS = 0
DEFAULT_PROPAGATED_ERRORS = 0
DEFAULT_STOPPED_ERRORS = 0


class SpanKindValues:
    """Numeric values for span kinds.

    Referenced by the SpanKind Enum in crisp.shared.models and by
    higher layers that emit span-kind-based classifications.
    """

    CLIENT = 0
    SERVER = 1
    UNKNOWN = 2
