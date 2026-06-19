"""Constants used throughout the critical path analysis system."""

# Jaeger JSON field names
SPANS = "spans"
SPAN_ID = "spanID"
REFERENCES = "references"
START_TIME = "startTime"
SPAN_KIND = "span.kind"
PEER_SERVICE = "peer.service"
DURATION = "duration"
OPERATION_NAME = "operationName"
PROCESS_ID = "processID"
TRACE_ID = "traceID"
REF_TYPE = "refType"
CHILD_OF = "CHILD_OF"
TAGS = "tags"
LOGS = "logs"
FIELDS = "fields"
PROCESSES = "processes"
HOSTNAME = "hostname"
TESTING = "testing"

# Analysis result field names
OP_TIME_EXCLUSIVE = "opTimeExclusive"
TOTAL_WORK = "totalWork"
TIME_SAVED_ON_WORK = "timeSavedOnWork"
TIME_SAVED_ON_CP = "timeSavedOnCP"
ERR_CP_CALLPATH_EXCLUSIVE = "errCPCallpathTimeExclusive"
ERR_CP_ERR_COUNTS = "errCPErrCounts"

# Span kind values
SERVER = "server"
CLIENT = "client"

# Parquet format field names
PARQUET_PROCESS = "process"
PARQUET_SERVICE_NAME = "service_name"
PARQUET_OPERATION_NAME = "operation_name"
PARQUET_HOSTNAME = "host_name"
PARQUET_START_TIME = "start_time_unix_nano"
PARQUET_DURATION = "duration_nano"
PARQUET_SPAN_SET = "span_set"
PARQUET_KIND = "kind"
PARQUET_SPAN_ID = "span_id"
PARQUET_PARENT_SPAN_ID = "parent_span_id"
PARQUET_SPANS = "spans"
PARQUET_TAGS = "StringTags"
PARQUET_ERROR = "error"
PARQUET_RPC_STATUS_CODE = "rpc_status_code"
PARQUET_RPC_SYSTEM = "rpc_system"
PARQUET_ERROR_MESSAGE = "error_message"

# Synthetic root names for error analysis
SYNTHETIC_ERR_CP_ROOT = "errorCriticalPathSyntheticRoot"
SYNTHETIC_FULL_ERR_NON_CP_ROOT = "fullErrorsNonCPSyntheticRoot"

# Terminal color codes for output formatting
class Colors:
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

# Magic numbers and defaults
DEFAULT_MIN_DEPTH = 1000000000
DEFAULT_MAX_DEPTH = -1

# Percentile calculation constants
PERCENTILE_50 = 0.5
PERCENTILE_90 = 0.9
PERCENTILE_95 = 0.95
PERCENTILE_99 = 0.99

# Default error count values
DEFAULT_SELF_ERRORS = 0
DEFAULT_PROPAGATED_ERRORS = 0
DEFAULT_STOPPED_ERRORS = 0

# Span kind enumeration values
class SpanKindValues:
    CLIENT = 0
    SERVER = 1
    UNKNOWN = 2

# Analysis type constants
CONST_TRACES_ANALYSIS_TYPE_LATENCY = "latency"
CONST_TRACES_ANALYSIS_TYPE_ERROR = "error"
