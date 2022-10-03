import flamegraph


def test_aggregateCCTs():
    input = [(1, {'a->b': 10, 'a->c': 30}), (2, {'a->b->c': 20}), (3, {'a->c': 30, 'a->b': 10})]
    expected = sorted('\na;b 20\na;b;c 20\na;c 60'.split('\n'))
    result = sorted(flamegraph.aggregateCCTs(input).split('\n'))
    assert result == expected
