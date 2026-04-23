from unittest import TestCase

from crisp.shared.utils import getLeafNodeFromCallPath


class TestGetLeafNodeFromCallPath(TestCase):
    def test_regular_path(self):
        path = "node1->node2->node3"
        result = getLeafNodeFromCallPath(path)
        self.assertEqual(result, "node3")

    def test_single_node_path(self):
        path = "singleNode"
        result = getLeafNodeFromCallPath(path)
        self.assertEqual(result, "singleNode")

    def test_empty_string(self):
        path = ""
        result = getLeafNodeFromCallPath(path)
        self.assertEqual(result, "")
