"""Dictionary utility functions for critical path analysis."""

from typing import Any


def accumulateInDict(dictName: dict[Any, Any], key: Any, value: Any) -> None:
    """Add value to existing key in dictName or insert new key and value.

    ``value`` must support ``__add__`` with the existing dict entry (e.g.
    numbers, or any custom class implementing ``__add__``). No copy is
    made; the accumulator is stored by reference.
    """
    if key in dictName:
        # add value to existing key in dictName
        dictName[key] = dictName[key] + value
    else:
        # insert new key and value into dictName
        dictName[key] = value


def maxExample(
    dictName: dict[Any, tuple[Any, Any]], key: Any, sid: Any, value: Any
) -> None:
    """Remember ``(sid, value)`` in ``dictName[key]`` if this is the worst case seen.

    Tie-break: comparison is strict ``>``, so the *first* ``sid`` written
    for a given ``value`` wins on equality. Callers that depend on a
    specific tie-break order should not rely on a later-inserted sid
    overriding an earlier one with the same value.
    """
    if key in dictName:
        if value > dictName[key][1]:
            # remember if this is the worst case seen
            dictName[key] = (sid, value)
    else:
        # remember since this is the first case seen
        dictName[key] = (sid, value)


def getCPSize(cct: dict[str, int]) -> int:
    """Calculate the size of the critical path profile string representation."""
    res = 0
    for k, v in cct.items():
        res += len("\n" + k.replace("->", ";") + " " + str(v))
    return res
