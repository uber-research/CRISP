"""Unit tests for crisp.utils.dict_utils."""

import unittest

from crisp.utils.dict_utils import accumulateInDict, getCPSize, maxExample


class TestDictUtils(unittest.TestCase):
    """Test cases for dictionary utility functions."""

    def test_accumulateInDict_new_key(self):
        """Test adding a new key to an empty dict."""
        test_dict = {}
        accumulateInDict(test_dict, "key1", 10)
        self.assertEqual(test_dict["key1"], 10)

    def test_accumulateInDict_existing_key(self):
        """Test accumulating to an existing key."""
        test_dict = {"key1": 5}
        accumulateInDict(test_dict, "key1", 10)
        self.assertEqual(test_dict["key1"], 15)

    def test_accumulateInDict_multiple_keys(self):
        """Test accumulating multiple keys."""
        test_dict = {}
        accumulateInDict(test_dict, "key1", 10)
        accumulateInDict(test_dict, "key2", 20)
        accumulateInDict(test_dict, "key1", 5)

        self.assertEqual(test_dict["key1"], 15)
        self.assertEqual(test_dict["key2"], 20)

    def test_accumulateInDict_with_objects(self):
        """Test accumulating with custom objects that support addition."""

        class TestObj:
            def __init__(self, value):
                self.value = value

            def __add__(self, other):
                return TestObj(self.value + other.value)

            def __eq__(self, other):
                return self.value == other.value

        test_dict = {}
        obj1 = TestObj(10)
        obj2 = TestObj(20)

        accumulateInDict(test_dict, "key1", obj1)
        accumulateInDict(test_dict, "key1", obj2)

        self.assertEqual(test_dict["key1"].value, 30)

    def test_maxExample_new_key(self):
        """Test adding a new key to maxExample dict."""
        test_dict = {}
        maxExample(test_dict, "key1", "span123", 100)
        self.assertEqual(test_dict["key1"], ("span123", 100))

    def test_maxExample_better_value(self):
        """Test updating with a better (higher) value."""
        test_dict = {"key1": ("span123", 50)}
        maxExample(test_dict, "key1", "span456", 100)
        self.assertEqual(test_dict["key1"], ("span456", 100))

    def test_maxExample_worse_value(self):
        """Test that worse (lower) values don't update."""
        test_dict = {"key1": ("span123", 100)}
        maxExample(test_dict, "key1", "span456", 50)
        self.assertEqual(test_dict["key1"], ("span123", 100))

    def test_maxExample_equal_value(self):
        """Test that equal values don't update."""
        test_dict = {"key1": ("span123", 100)}
        maxExample(test_dict, "key1", "span456", 100)
        self.assertEqual(test_dict["key1"], ("span123", 100))

    def test_getCPSize_empty_dict(self):
        """Test getCPSize with empty dict."""
        result = getCPSize({})
        self.assertEqual(result, 0)

    def test_getCPSize_single_entry(self):
        """Test getCPSize with a single entry."""
        test_dict = {"service->operation": 100}
        # Expected: "\nservice;operation 100" = 21 characters
        expected_length = len("\nservice;operation 100")
        result = getCPSize(test_dict)
        self.assertEqual(result, expected_length)

    def test_getCPSize_multiple_entries(self):
        """Test getCPSize with multiple entries."""
        test_dict = {"service1->operation1": 100, "service2->operation2": 200}
        # Expected: "\nservice1;operation1 100" + "\nservice2;operation2 200"
        expected_length = len("\nservice1;operation1 100") + len(
            "\nservice2;operation2 200"
        )
        result = getCPSize(test_dict)
        self.assertEqual(result, expected_length)

    def test_getCPSize_arrow_replacement(self):
        """Test that arrows are properly replaced with semicolons."""
        test_dict = {"a->b->c": 123}
        # Expected: "\na;b;c 123"
        expected_length = len("\na;b;c 123")
        result = getCPSize(test_dict)
        self.assertEqual(result, expected_length)

    def test_getCPSize_large_numbers(self):
        """Test getCPSize with large numbers."""
        test_dict = {"service->operation": 123456789}
        # Expected: "\nservice;operation 123456789"
        expected_length = len("\nservice;operation 123456789")
        result = getCPSize(test_dict)
        self.assertEqual(result, expected_length)


if __name__ == "__main__":
    unittest.main()
