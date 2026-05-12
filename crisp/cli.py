# ruff: noqa: I001
import argparse
from dataclasses import dataclass
from typing import Optional
from collections.abc import Sequence


@dataclass(frozen=True)
class MainArgs:
    services_file: str
    numTrace: int
    ioParallelism: int
    computeParallelism: int
    shardId: int
    numShards: int
    diskRequirement: int
    endpointDiskGBLimit: int
    useMP: bool
    uploadToTB: bool
    uploadToCrispRiTB: bool
    ignoreCtfTests: bool
    useMidnightTime: bool
    ignoreLastNMinutes: int
    filterProxy: bool
    useUSSO: bool
    mergeAllRoots: bool
    doRanges: bool
    uploadTar: bool
    noOverwriteUpload: bool
    qps: int
    emitM3Metrics: bool
    jobTag: Optional[str]
    errorAnalysis: bool
    useParquet: bool
    startTimestamp: Optional[int]
    endTimestamp: Optional[int]
    maxExemplars: int


def _build_main_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--services-file",
        "-s",
        dest="services_file",
        type=argparse.FileType("r"),
        required=True,
        help="A YAML file with a list of service and operation names.",
    )
    parser.add_argument(
        "--numTrace",
        action="store",
        help="number of traces to download",
        default=1000,
        type=int,
    )
    parser.add_argument(
        "--ioParallelism",
        action="store",
        help="Number of concurrent python processes.",
        default=1,
        type=int,
    )
    parser.add_argument(
        "--computeParallelism",
        action="store",
        help="Number of concurrent python processes.",
        default=1,
        type=int,
    )
    parser.add_argument(
        "--shardId",
        action="store",
        help="The shards id of this shard.",
        default=0,
        type=int,
    )
    parser.add_argument(
        "--numShards",
        action="store",
        help="Number of shards that the yaml is chunked into.",
        default=1,
        type=int,
    )
    parser.add_argument(
        "--diskRequirement",
        action="store",
        help="disk requirement to run CRISP in Gigabyte.",
        default=5,
        type=int,
    )
    parser.add_argument(
        "--endpointDiskGBLimit",
        action="store",
        help="upperbound for individual endpoint disk usage in Gigabyte.",
        default=500,
        type=int,
    )
    parser.add_argument(
        "--useMP",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use multiprocessing based pipeline",
    )
    parser.add_argument(
        "--uploadToTB",
        dest="uploadToTB",
        action="store_true",
        default=False,
        required=False,
        help="Upload the data to terrablob.",
    )
    parser.add_argument(
        "--uploadToCrispRiTB",
        dest="uploadToCrispRiTB",
        action="store_true",
        default=False,
        required=False,
        help="Upload the data to Crisp RI terrablob.",
    )
    parser.add_argument(
        "--ignoreCtfTests",
        dest="ignoreCtfTests",
        action="store_true",
        help="ignore traces that contains ctf-tests",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--useMidnightTime",
        dest="useMidnightTime",
        action="store_true",
        default=False,
        required=False,
        help="Use the start time as the midnight today UTC.",
    )
    parser.add_argument(
        "--ignoreLastNMinutes",
        action="store",
        help="Number of last N minutes to ignore for trace collection.",
        default=10,
        type=int,
    )
    parser.add_argument(
        "--filterProxy",
        dest="filterProxy",
        action="store_true",
        default=False,
        required=False,
        help="Remove proxy nodes from the output",
    )
    parser.add_argument(
        "--useUSSO",
        dest="useUSSO",
        action="store_true",
        default=False,
        required=False,
        help="Use usso for fetching traces from jaeger.",
    )
    parser.add_argument(
        "--mergeAllRoots",
        dest="mergeAllRoots",
        action=argparse.BooleanOptionalAction,
        default=True,
        required=False,
        help="Merge metrics from every matching root span instead of analyzing only the first match (default=true).",
    )
    parser.add_argument(
        "--doRanges",
        dest="doRanges",
        action="store_true",
        help="Compute flamegraphs for every 20 percentiles (default=false)",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--uploadTar",
        dest="uploadTar",
        action="store_true",
        help="Upload all the trace in tar.gz format (default=false). Requires uploadToTB to be set",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--noOverwriteUpload",
        dest="noOverwriteUpload",
        action="store_true",
        help="Do not upload if path is already present in RI terrablob (default=false).",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--qps",
        dest="qps",
        action="store",
        help="QPS for the trace fetching (default=500)",
        default=500,
        type=int,
    )
    parser.add_argument(
        "--emitM3Metrics",
        dest="emitM3Metrics",
        action="store_true",
        default=False,
        required=False,
        help="Emit metrics to M3.",
    )
    parser.add_argument(
        "--jobTag",
        dest="jobTag",
        action="store",
        required=False,
        help="The tag of the job (value will be used as a 'job' tag in M3 metrics).",
        type=str,
    )
    parser.add_argument(
        "--errorAnalysis",
        dest="errorAnalysis",
        action="store_true",
        default=False,
        required=False,
        help="Run error analysis",
    )
    parser.add_argument(
        "--useParquet",
        dest="useParquet",
        action="store_true",
        default=False,
        required=False,
        help="Use parquet format for storing traces",
    )
    parser.add_argument(
        "--startTimestamp",
        dest="startTimestamp",
        action="store",
        default=None,
        required=False,
        help="Start timestamp for trace collection in UTC format",
        type=int,
    )
    parser.add_argument(
        "--endTimestamp",
        dest="endTimestamp",
        action="store",
        default=None,
        required=False,
        help="End timestamp for trace collection in UTC format",
        type=int,
    )
    parser.add_argument(
        "--maxExemplars",
        dest="maxExemplars",
        action="store",
        default=3,
        required=False,
        help="Maximum number of exemplars (trace/span pairs) to keep per call path in protobuf output (default=3).",
        type=int,
    )
    return parser


def parse_main_args(argv: Optional[Sequence[str]] = None) -> MainArgs:
    parser = _build_main_parser()
    namespace = parser.parse_args(argv)
    services_handle = namespace.services_file
    services_path = services_handle.name
    services_handle.close()
    return MainArgs(
        services_file=services_path,
        numTrace=namespace.numTrace,
        ioParallelism=namespace.ioParallelism,
        computeParallelism=namespace.computeParallelism,
        shardId=namespace.shardId,
        numShards=namespace.numShards,
        diskRequirement=namespace.diskRequirement,
        endpointDiskGBLimit=namespace.endpointDiskGBLimit,
        useMP=namespace.useMP,
        uploadToTB=namespace.uploadToTB,
        uploadToCrispRiTB=namespace.uploadToCrispRiTB,
        ignoreCtfTests=namespace.ignoreCtfTests,
        useMidnightTime=namespace.useMidnightTime,
        ignoreLastNMinutes=namespace.ignoreLastNMinutes,
        filterProxy=namespace.filterProxy,
        useUSSO=namespace.useUSSO,
        mergeAllRoots=namespace.mergeAllRoots,
        doRanges=namespace.doRanges,
        uploadTar=namespace.uploadTar,
        noOverwriteUpload=namespace.noOverwriteUpload,
        qps=namespace.qps,
        emitM3Metrics=namespace.emitM3Metrics,
        jobTag=namespace.jobTag,
        errorAnalysis=namespace.errorAnalysis,
        useParquet=namespace.useParquet,
        startTimestamp=namespace.startTimestamp,
        endTimestamp=namespace.endTimestamp,
        maxExemplars=namespace.maxExemplars,
    )
