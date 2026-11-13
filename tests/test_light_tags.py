"""Tag filters in the light pipeline: per-root matching and the per-tag flame
graphs (see Graph.matchingTags, lightProcess and CONFORMANCE.md, "Per-tag
outputs").

Mirrors go/crisp/light_tags_test.go on the same traces.
"""
import json

import pytest

import crisp.common as common
from crisp.fragment import pruneToFragment
from crisp.process_trace import checkTagFilters, lightProcess, process, processRoots, tagOutputPrefix

LIGHT_CCT = "light-flame-graph-P100.cct"


def span(spanId, op, pid, parent=None, start=1000, dur=100, **tags):
    refs = [{"traceID": "T1", "spanID": parent, "refType": "CHILD_OF"}] if parent else []
    return {
        "traceID": "T1", "spanID": spanId, "operationName": op, "references": refs,
        "startTime": start, "duration": dur, "processID": pid,
        "tags": [{"key": k, "type": "string", "value": v} for k, v in tags.items()],
    }


def trace(*spans):
    services = {"pA": "svcA", "pB": "svcB", "pC": "svcC"}
    return {"data": [{
        "traceID": "T1",
        "spans": list(spans),
        "processes": {pid: {"serviceName": name} for pid, name in services.items()},
    }]}


def tag(name, value, depth=1):
    return {"name": name, "value": value, "search_depth": depth}


ENV_PROD = tag("env", "prod")
REGION_WEST = tag("region", "west", 2)
REGION_WEST_ROOT = tag("region", "west", 1)
TIER_ONE = tag("tier", "1", 3)

# Three analysis roots, each a trace root matching svcA/opA.
#   A (100us) has env=prod. Its child A1 has region=west, and A1's child A2 has tier=1.
#   B (200us) has region=west itself.
#   C (300us) has no tag. Its child C1 starts after C ended, so sanitization
#   drops it: its region=west does not count.
ROOTS = [
    span("A", "opA", "pA", start=0, dur=100, env="prod"),
    span("A1", "opB", "pB", "A", start=10, dur=80, region="west"),
    span("A2", "opC", "pC", "A1", start=20, dur=50, tier="1"),
    span("B", "opA", "pA", start=1000, dur=200, region="west"),
    span("C", "opA", "pA", start=2000, dur=300),
    span("C1", "opB", "pB", "C", start=3000, dur=50, region="west"),
]


def config(tags, **overrides):
    args = dict(serviceName="svcA", operationName="opA", rootTrace=False, lightMode=True, tags=tags)
    args.update(overrides)
    return common.Config(**args)


def write(tmp_path, spans):
    path = tmp_path / "T1.json"
    path.write_text(json.dumps(trace(*spans)))
    return str(path)


def matched(tmp_path, tags, **overrides):
    metrics = processRoots(write(tmp_path, ROOTS), config(tags, **overrides))
    return {m.rootSpanID: m.tags for m in metrics}


# --- which filters match under a root -------------------------------------------

def test_tags_are_matched_under_each_root(tmp_path):
    assert matched(tmp_path, [ENV_PROD, REGION_WEST]) == {
        "A": [ENV_PROD, REGION_WEST],
        "B": [REGION_WEST],
        "C": [],
    }


def test_tag_search_depth_counts_levels_from_the_root(tmp_path):
    # A1 is at level 2 and A2 at level 3.
    assert matched(tmp_path, [REGION_WEST_ROOT, tag("tier", "1", 2), TIER_ONE]) == {
        "A": [TIER_ONE],
        "B": [REGION_WEST_ROOT],
        "C": [],
    }


def test_tags_without_merge_all_roots_match_the_first_root(tmp_path):
    assert matched(tmp_path, [ENV_PROD, REGION_WEST], mergeAllRoots=False) == {"A": [ENV_PROD, REGION_WEST]}


def test_tags_are_matched_in_the_heavy_pipeline_too(tmp_path):
    metrics = process(write(tmp_path, ROOTS), config([ENV_PROD, REGION_WEST_ROOT], lightMode=False))
    assert metrics.tags == [ENV_PROD]


def test_tag_value_must_be_equal(tmp_path):
    assert matched(tmp_path, [tag("env", "pro"), tag("ENV", "prod"), tag("tier", 1, 3)]) == {"A": [], "B": [], "C": []}


# --- per-tag outputs ------------------------------------------------------------

def run_light(tmp_path, spans, tags, **overrides):
    traces = tmp_path / "traces"
    traces.mkdir(parents=True)
    path = traces / "T1.json"
    path.write_text(json.dumps(spans if isinstance(spans, dict) else trace(*spans)))
    out = tmp_path / "out"
    out.mkdir()
    c = config(tags, **overrides)
    c.jaegerTraceFiles = [str(path)]
    c.getOutputDir = lambda: str(out)
    lightProcess(c)
    return {p.name: p.read_bytes() for p in sorted(out.iterdir())}


def test_each_tag_filter_gets_its_own_flame_graph(tmp_path):
    outputs = run_light(tmp_path, ROOTS, [ENV_PROD, REGION_WEST, tag("env", "test")])

    def lines(prefix):
        return sorted(outputs[prefix + LIGHT_CCT].decode().splitlines())

    # All three roots. Times are averaged over the roots: (20 + 200 + 300) // 3.
    assert lines("") == [
        "[svcA] opA 173 <<3>>",
        "[svcA] opA;[svcB] opB 10 <<1>>",
        "[svcA] opA;[svcB] opB;[svcC] opC 16 <<1>>",
    ]
    # env=prod: root A only.
    assert lines("env_prod_") == [
        "[svcA] opA 20 <<1>>",
        "[svcA] opA;[svcB] opB 30 <<1>>",
        "[svcA] opA;[svcB] opB;[svcC] opC 50 <<1>>",
    ]
    # region=west within two levels: roots A and B.
    assert lines("region_west_") == [
        "[svcA] opA 110 <<2>>",
        "[svcA] opA;[svcB] opB 15 <<1>>",
        "[svcA] opA;[svcB] opB;[svcC] opC 25 <<1>>",
    ]
    # A filter that matches no root still writes its files.
    assert outputs["env_test_" + LIGHT_CCT] == b""
    for prefix in ("", "env_prod_", "region_west_", "env_test_"):
        for name in (LIGHT_CCT, "light-flame-graph-P100.dot", "light-flame-graph-P100.pb"):
            assert prefix + name in outputs


def test_tag_filters_do_not_change_the_other_outputs(tmp_path):
    tagged = run_light(tmp_path / "tags", ROOTS, [ENV_PROD, REGION_WEST], conformance=True)
    plain = run_light(tmp_path / "plain", ROOTS, [], conformance=True)
    assert {name: tagged[name] for name in plain} == plain
    assert sorted(set(tagged) - set(plain)) == sorted(
        prefix + name
        for prefix in ("env_prod_", "region_west_")
        for name in (LIGHT_CCT, "light-flame-graph-P100.dot", "light-flame-graph-P100.pb",
                     "conformance.cct", "conformance.json")
    )


def test_per_tag_outputs_follow_the_cohort_filters(tmp_path):
    # Only B (200us) is inside the window.
    outputs = run_light(tmp_path, ROOTS, [REGION_WEST], minDuration=150, maxDuration=250)
    assert outputs["region_west_" + LIGHT_CCT] == b"[svcA] opA 200 <<1>>\n"
    assert outputs[LIGHT_CCT] == outputs["region_west_" + LIGHT_CCT]


def test_per_tag_outputs_of_a_fragment_are_those_of_its_trace(tmp_path):
    spans = [span("X", "opX", "pC", start=0, dur=5000), span("U", "opU", "pC", "X", start=10, dur=10, env="prod")]
    spans += [dict(s, references=s["references"] or [{"traceID": "T1", "spanID": "X", "refType": "CHILD_OF"}])
              for s in ROOTS[:4]]
    data = trace(*spans)
    tags = [ENV_PROD, REGION_WEST, TIER_ONE]
    full = run_light(tmp_path / "full", data, tags, fragment=True, conformance=True)
    pruned = run_light(tmp_path / "pruned", pruneToFragment(data, "svcA", "opA"), tags, fragment=True, conformance=True)
    assert pruned == full
    assert full["env_prod_" + LIGHT_CCT]


# --- file names -----------------------------------------------------------------

def test_tag_output_prefix():
    assert tagOutputPrefix(tag("env", "prod")) == "env_prod_"
    assert tagOutputPrefix(tag("http.status_code", "200")) == "http_status_code_200_"
    assert tagOutputPrefix(tag("a b", "c/d")) == "a_b_c_d_"


def test_tag_filters_with_the_same_prefix_are_rejected(tmp_path):
    c = config([tag("a.b", "c"), tag("a", "b.c")])
    with pytest.raises(ValueError, match="same output file prefix"):
        checkTagFilters(c)
    c.jaegerTraceFiles = []
    with pytest.raises(ValueError, match="same output file prefix"):
        lightProcess(c)
    checkTagFilters(config([tag("a.b", "c"), tag("a", "c")]))
