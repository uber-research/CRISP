from pathlib import Path

import crisp.cli as cli


def _get_services_file() -> str:
    return str(Path(__file__).resolve().parents[1] / "services.yaml")


def test_parse_main_args_defaults():
    services_path = _get_services_file()
    args = cli.parse_main_args(["--services-file", services_path])
    assert args.services_file == services_path
    assert args.numTrace == 1000
    assert args.ioParallelism == 1
    assert args.computeParallelism == 1
    assert args.shardId == 0
    assert args.numShards == 1
    assert args.diskRequirement == 5
    assert args.endpointDiskGBLimit == 500
    assert args.useMP is False
    assert args.useMidnightTime is False
    assert args.ignoreLastNMinutes == 10
    assert args.filterProxy is False
    assert args.mergeAllRoots is True
    assert args.doRanges is False
    assert args.qps == 500
    assert args.errorAnalysis is False
    assert args.startTimestamp is None
    assert args.endTimestamp is None
    assert args.maxExemplars == 3


def test_parse_main_args_overrides():
    services_path = _get_services_file()
    argv = [
        "--services-file",
        services_path,
        "--numTrace",
        "123",
        "--ioParallelism",
        "4",
        "--computeParallelism",
        "5",
        "--shardId",
        "2",
        "--numShards",
        "3",
        "--diskRequirement",
        "7",
        "--endpointDiskGBLimit",
        "9",
        "--useMP",
        "--useMidnightTime",
        "--ignoreLastNMinutes",
        "11",
        "--filterProxy",
        "--no-mergeAllRoots",
        "--doRanges",
        "--qps",
        "321",
        "--errorAnalysis",
        "--startTimestamp",
        "1",
        "--endTimestamp",
        "2",
        "--maxExemplars",
        "5",
    ]

    args = cli.parse_main_args(argv)

    assert args.numTrace == 123
    assert args.ioParallelism == 4
    assert args.computeParallelism == 5
    assert args.shardId == 2
    assert args.numShards == 3
    assert args.diskRequirement == 7
    assert args.endpointDiskGBLimit == 9
    assert args.useMP is True
    assert args.useMidnightTime is True
    assert args.ignoreLastNMinutes == 11
    assert args.filterProxy is True
    assert args.mergeAllRoots is False
    assert args.doRanges is True
    assert args.qps == 321
    assert args.errorAnalysis is True
    assert args.startTimestamp == 1
    assert args.endTimestamp == 2
    assert args.maxExemplars == 5
