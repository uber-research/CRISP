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
