class BaseCrispException(Exception):
    """Base CRISP exception"""


class GetTraceBaseException(BaseCrispException):
    """Base exception for get trace phase"""


class NoTracesDownloadedException(GetTraceBaseException):
    """Raised in case no traces were downloaded during get trace phase"""


class NoTraceIDsFoundException(GetTraceBaseException):
    """Raised in case no trace IDs were found withing specified time range during get trace phase"""


class TBPathExistsException(BaseCrispException):
    """Raised in case TB directory already exists"""


class BKJobSchedulerException(BaseCrispException):
    """Raised in case the HTTP service fails to schedule the BK job in background"""
