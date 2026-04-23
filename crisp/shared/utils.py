"""Shared utility functions used across critical path analysis modules.

This module contains simple utility functions that are used by multiple
modules to avoid code duplication and circular dependencies.
"""


def getLeafNodeFromCallPath(path: str) -> str:
    """Extract the leaf node from a call path string.

    Args:
        path: Call path string with nodes separated by '->'

    Returns:
        The last node in the path

    Examples:
        >>> getLeafNodeFromCallPath("service1->service2->service3")
        'service3'
        >>> getLeafNodeFromCallPath("single_node")
        'single_node'
    """
    return path.rsplit("->", 1)[-1]
