import argparse
import copy
import yaml


def read_yaml(file_name):
    """Read a YAML file and return its content."""
    with open(file_name) as file:
        return yaml.safe_load(file)


def merge_without_overwrite_or_duplication(dict1, dict2):
    """
    Merges two dictionaries without overwriting or duplicating entries,
    specifically handling list and dict values.
    """
    merged = copy.deepcopy(dict1)

    for key, val_list in dict2.items():
        if key not in merged:
            # If key doesn't exist in dict1, simply add it
            merged[key] = val_list
        else:
            # Key exists in dict1, so we need to merge the values
            if isinstance(merged[key], list) and isinstance(val_list, list):
                for item in val_list:
                    if item not in merged[key]:
                        merged[key].append(item)
    return merged


def write_yaml(data, file_name):
    """Write data to a YAML file."""
    with open(file_name, "w") as file:
        yaml.dump(data, file)


def merge_yaml(file1_name, file2_name, merged_file_name):
    """Merge two YAML files and write the result to a new file."""
    # Read the content of both YAML files
    file1_content = read_yaml(file1_name)
    file2_content = read_yaml(file2_name)

    # Merge the contents without overwriting or duplicating
    merged_content = merge_without_overwrite_or_duplication(file1_content, file2_content)

    # Write the merged content to the output YAML file
    write_yaml(merged_content, merged_file_name)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Merge two YAML files into a single YAML file.",
    )

    # Add explicit arguments for input file names and output file name
    parser.add_argument("--file1", required=True, help="First YAML file to merge")
    parser.add_argument("--file2", required=True, help="Second YAML file to merge")
    parser.add_argument(
        "--merged",
        required=True,
        help="Output file name for the merged YAML",
    )

    # Parse the arguments
    return parser.parse_args()


def main():
    """Main function to execute the script logic."""
    args = parse_arguments()

    # Get file names from parsed arguments
    file1_name = args.file1
    file2_name = args.file2
    merged_file_name = args.merged

    # Merge the YAML files
    merge_yaml(file1_name, file2_name, merged_file_name)


if __name__ == "__main__":
    main()
