"""Storage and upload functions for critical path analysis results.

This module handles uploading analysis results to S3-compatible storage.
"""

import logging
import multiprocessing as mp
import os
import subprocess
import time
from datetime import datetime

import crisp.common as common
from crisp.shared.models import Metrics
from crisp.tb_client import TBClient


def _noop_analytics_upload(*args, **kwargs):
    raise NotImplementedError("Analytics upload not supported in open-source build")


# Constants for storage paths
DATE_TIME = datetime.now().strftime("%d_%B_%Y_%H_%M_%S")
TBPATH = "/crisp/"
CRISP_SECONDARY_DATE_TIME = datetime.now().strftime("%Y_%m_%d")
CRISP_SECONDARY_TBPATH = "/crisp_secondary/"


def compressAndUpload(
    service,
    operation,
    directory,
    blobPath,
    dateTime,
    publishAsLatest=False,
):
    """Compress the entire trace directory and upload to storage."""
    if directory.endswith("/"):
        directory = directory[:-1]

    tgzFile = directory + ".tgz"
    tarCmd = ("tar", "-c", "--use-compress-program=pigz", "-f", tgzFile, directory)
    try:
        subprocess.check_call(tarCmd)
        tb_path = common.serviceOperationToTBPath(service, operation, blobPath, dateTime)
        TBClient().upload_file_to_tb(tb_path, tgzFile)
    except Exception:
        logging.warning("Storage upload failed.")
    finally:
        # Remove the tgz file.
        if os.path.exists(tgzFile):
            os.remove(tgzFile)


def Upload(serviceName, operationName, files, blobPath, dateTime):
    """Upload multiple files to storage."""
    for f in files:
        tb_path = common.serviceOperationToTBPath(serviceName, operationName, blobPath, dateTime)
        TBClient().upload_file_to_tb(tb_path, f)


def TBPathExists(tbPath):
    """Check if a storage path exists."""
    client = TBClient()

    return client.check_if_dir_exists(path=tbPath)


def uploadReal(c: common.Config):
    """Upload analysis results to storage paths."""
    # tar the entire trace dir
    compressAndUpload(
        c.serviceName,
        c.operationName,
        c.tracesDir,
        TBPATH,
        DATE_TIME,
        publishAsLatest=False,
    )
    Upload(c.serviceName, c.operationName, c.filesToUpload, TBPATH, DATE_TIME)

    tbPath = common.serviceOperationToTBPath(
        c.serviceName,
        c.operationName,
        CRISP_SECONDARY_TBPATH,
        CRISP_SECONDARY_DATE_TIME,
    )
    absent = not TBPathExists(tbPath)
    if absent:
        Upload(
            c.serviceName,
            c.operationName,
            c.filesToUpload,
            CRISP_SECONDARY_TBPATH,
            CRISP_SECONDARY_DATE_TIME,
        )
    else:
        logging.info(
            "[%s]%s storage path %s already exists, skipping secondary upload",
            c.serviceName,
            c.operationName,
            tbPath,
        )

    return 0


def uploadWrapper(c: common.Config, resultQ: mp.Queue) -> common.Config:
    """Wrapper for upload step in multiprocessing pipeline."""
    return common.templateHandler(
        message="upload step",
        realHandler=uploadReal,
        preStart=None,
        postFinish=None,
        c=c,
        resultQ=resultQ,
    )


def uploadCrossRegionCalls(metrics: list[Metrics], c: common.Config, table_name: str = "critical_path_cross_region_calls"):
    """
    Upload cross-region calls data for analysis.

    Note: Not implemented in the open-source build. Override this function
    to integrate with your own analytics storage backend.

    Args:
        metrics: List of Metrics objects containing cross-region call data
        c: Configuration object
        table_name: Table name for the data (default: critical_path_cross_region_calls)

    Returns:
        str: Upload result status ("success", "error", "timeout", "partial")
    """
    raise NotImplementedError("uploadCrossRegionCalls not implemented in open-source build")
