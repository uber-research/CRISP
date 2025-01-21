import os
import tempfile
import unittest
from unittest.mock import patch
import yaml

# Import all functions to be tested
from ..yaml_merger import (
    main,
    merge_yaml,
    merge_without_overwrite_or_duplication,
    read_yaml,
    write_yaml,
    parse_arguments,
)


class TestMergedYAML(unittest.TestCase):
    def setUp(self):
        # Create temporary YAML files with sample data for testing
        self.file1_content = {
            "key1": ["value1"],
            "key2": ["value2"],
            "key_shared": ["shared_value1"],
        }
        self.file2_content = {
            "key3": ["value3"],
            "key4": ["value4"],
            "key_shared": ["shared_value1", "shared_value2"],
        }

        # Write content to temporary files
        self.file1 = tempfile.NamedTemporaryFile(mode="w", delete=False)
        self.file2 = tempfile.NamedTemporaryFile(mode="w", delete=False)
        yaml.dump(self.file1_content, self.file1)
        yaml.dump(self.file2_content, self.file2)
        self.file1.close()
        self.file2.close()

    def tearDown(self):
        # Remove the temporary files after testing
        os.remove(self.file1.name)
        os.remove(self.file2.name)

    def test_merge_without_overwrite_or_duplication(self):
        """Test merging dictionaries without overwriting or duplicating entries."""
        expected_merged_content = {
            "key1": ["value1"],
            "key2": ["value2"],
            "key3": ["value3"],
            "key4": ["value4"],
            "key_shared": ["shared_value1", "shared_value2"],
        }

        result = merge_without_overwrite_or_duplication(
            self.file1_content,
            self.file2_content,
        )

        self.assertEqual(result, expected_merged_content)

    def test_merge_yaml_function(self):
        """Test the merge_yaml function directly."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as merged_file:
            merged_file_name = merged_file.name

        try:
            merge_yaml(self.file1.name, self.file2.name, merged_file_name)

            with open(merged_file_name) as f:
                merged_content = yaml.safe_load(f)

            expected_merged_content = {
                "key1": ["value1"],
                "key2": ["value2"],
                "key3": ["value3"],
                "key4": ["value4"],
                "key_shared": ["shared_value1", "shared_value2"],
            }

            self.assertEqual(merged_content, expected_merged_content)
        finally:
            os.remove(merged_file_name)

    def test_main_function(self):
        """Test the main function with command-line arguments."""
        # Prepare arguments
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as merged_file:
            merged_file_name = merged_file.name

        args = [
            '--file1', self.file1.name,
            '--file2', self.file2.name,
            '--merged', merged_file_name,
        ]

        # Patch sys.argv
        with patch('sys.argv', ["program_name", *args]):
            main()

            with open(merged_file_name) as f:
                merged_content = yaml.safe_load(f)

            expected_merged_content = {
                "key1": ["value1"],
                "key2": ["value2"],
                "key3": ["value3"],
                "key4": ["value4"],
                "key_shared": ["shared_value1", "shared_value2"],
            }

            self.assertEqual(merged_content, expected_merged_content)

        os.remove(merged_file_name)

    def test_read_yaml(self):
        """Test reading YAML files."""
        # Test reading a valid YAML file
        content = read_yaml(self.file1.name)
        self.assertEqual(content, self.file1_content)

        # Test reading a non-existent file
        with self.assertRaises(FileNotFoundError):
            read_yaml("non_existent_file.yaml")

        # Test reading a malformed YAML file
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            temp_file.write("invalid: [unclosed")
            temp_file_name = temp_file.name

        try:
            with self.assertRaises(yaml.YAMLError):
                read_yaml(temp_file_name)
        finally:
            os.remove(temp_file_name)

    def test_write_yaml(self):
        """Test writing data to a YAML file."""
        data = {"key": ["value"]}
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            temp_file_name = temp_file.name

        try:
            write_yaml(data, temp_file_name)

            with open(temp_file_name) as f:
                content = yaml.safe_load(f)

            self.assertEqual(content, data)
        finally:
            os.remove(temp_file_name)

    def test_parse_arguments(self):
        """Test parsing command-line arguments."""
        args = [
            '--file1', 'file1.yaml',
            '--file2', 'file2.yaml',
            '--merged', 'merged.yaml',
        ]
        with patch('sys.argv', ["program_name", *args]):
            parsed_args = parse_arguments()
            self.assertEqual(parsed_args.file1, 'file1.yaml')
            self.assertEqual(parsed_args.file2, 'file2.yaml')
            self.assertEqual(parsed_args.merged, 'merged.yaml')

    def test_merge_with_non_list_values(self):
        """Test merging dictionaries where values are not lists."""
        dict1 = {"key1": "value1"}
        dict2 = {"key1": "value2"}
        expected_result = {"key1": "value1"}  # Original value preserved

        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_mixed_types(self):
        """Test merging dictionaries with mixed value types."""
        dict1 = {"key1": ["value1"]}
        dict2 = {"key1": "value2"}
        expected_result = {"key1": ["value1"]}  # Original value preserved

        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_yaml_invalid_files(self):
        """Test merging when one of the files does not exist."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump({"key": "value"}, temp_file)
            existing_file = temp_file.name

        non_existent_file = "non_existent_file.yaml"
        output_file = tempfile.NamedTemporaryFile(mode="w", delete=False).name

        try:
            with self.assertRaises(FileNotFoundError):
                merge_yaml(existing_file, non_existent_file, output_file)
        finally:
            os.remove(existing_file)
            os.remove(output_file)

    def test_merge_yaml_malformed_yaml(self):
        """Test merging when one of the YAML files is malformed."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file1:
            yaml.dump({"key1": "value1"}, temp_file1)
            file1_name = temp_file1.name

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file2:
            temp_file2.write("not: [valid yaml")
            file2_name = temp_file2.name

        output_file = tempfile.NamedTemporaryFile(mode="w", delete=False).name

        try:
            with self.assertRaises(yaml.YAMLError):
                merge_yaml(file1_name, file2_name, output_file)
        finally:
            os.remove(file1_name)
            os.remove(file2_name)
            os.remove(output_file)

    def test_merge_empty_dicts(self):
        """Test merging two empty dictionaries."""
        dict1 = {}
        dict2 = {}
        expected_result = {}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_one_empty_dict(self):
        """Test merging when one dictionary is empty."""
        dict1 = {"key1": ["value1"]}
        dict2 = {}
        expected_result = {"key1": ["value1"]}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

        dict1 = {}
        dict2 = {"key2": ["value2"]}
        expected_result = {"key2": ["value2"]}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_none_values(self):
        """Test merging dictionaries with None values."""
        dict1 = {"key1": None}
        dict2 = {"key1": None}
        expected_result = {"key1": None}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_different_types(self):
        """Test merging dictionaries with different value types for the same key."""
        dict1 = {"key1": ["value1"]}
        dict2 = {"key1": {"subkey": "subvalue"}}
        expected_result = {"key1": ["value1"]}  # Original value preserved
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_nested_lists(self):
        """Test merging dictionaries where values are nested lists."""
        dict1 = {"key1": [["value1"]]}
        dict2 = {"key1": [["value2"]]}
        expected_result = {"key1": [["value1"], ["value2"]]}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_duplicate_items_in_list(self):
        """Test that duplicate items are not added when merging lists."""
        dict1 = {"key1": ["value1", "value2"]}
        dict2 = {"key1": ["value2", "value3"]}
        expected_result = {"key1": ["value1", "value2", "value3"]}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_non_list_and_list(self):
        """Test merging when one value is a list and the other is not."""
        dict1 = {"key1": "value1"}
        dict2 = {"key1": ["value2"]}
        expected_result = {"key1": "value1"}  # Original value preserved
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_additional_data_types(self):
        """Test merging with various data types."""
        dict1 = {"key1": 123}
        dict2 = {"key1": 456}
        expected_result = {"key1": 123}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

        dict1 = {"key1": True}
        dict2 = {"key1": False}
        expected_result = {"key1": True}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)

    def test_merge_with_complex_nested_structures(self):
        """Test merging with complex nested structures."""
        dict1 = {"key1": [{"subkey1": ["value1"]}]}
        dict2 = {"key1": [{"subkey1": ["value2"]}]}
        expected_result = {"key1": [{"subkey1": ["value1"]}, {"subkey1": ["value2"]}]}
        result = merge_without_overwrite_or_duplication(dict1, dict2)
        self.assertEqual(result, expected_result)
