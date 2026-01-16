"""Includes test cases for all the functions at /utils/tree_helper.py"""

from utils.tree_helper import (
    get_folder_icon,
    get_prefix,
    get_files_and_directories,
    get_full_path,
)


############ get_folder_icon ############
# 3 valid case
def test_get_folder_icon_empty_valid():
    """
    Test case for the get_folder_icon function when the parameter `is_last` is empty
    """
    assert get_folder_icon(is_last="") == "├── "


def test_get_folder_icon_true_valid():
    """
    Test case for the get_folder_icon function when the parameter `is_last` is True
    """
    assert get_folder_icon(True) == "└── "


def test_get_folder_icon_false_valid():
    """
    Test case for the get_folder_icon function when the parameter `is_last` is False
    """
    assert get_folder_icon(False) == "├── "


############ get_prefix ############
# 3 valid case
def test_get_prefix_empty_valid():
    """
    Test case for the get_prefix function when the parameter `is_last` is empty
    """
    assert get_prefix(is_last="") == "│   "


def test_get_prefix_true_valid():
    """
    Test case for the get_prefix function when the parameter `is_last` is True
    """
    assert get_prefix(True) == "    "


def test_get_prefix_false_valid():
    """
    Test case for the get_prefix function when the parameter `is_last` is False
    """
    assert get_prefix(False) == "│   "


############ get_files_and_directories ############
# 1 valid case
# 2 invalid case


def test_get_files_and_directories_empty_invalid():
    """
    Test case for the get_files_and_directories function when the parameter `path` is empty
    """
    assert get_files_and_directories("") == []


def test_get_files_and_directories_file_path_invalid():
    """
    Test case for the get_files_and_directories function when the parameter `path` is a file path
    """
    assert get_files_and_directories("test_tree_helper.py") == []


def test_get_files_and_directories_folder_path_valid():
    """
    Test case for the get_files_and_directories function when the parameter `path` is a folder path
    """
    result = get_files_and_directories(
        "/Users/sidd/Documents/github/gos/tests/templates"
    )
    print(result)
    assert result == [
        "config_empty_values.json",
        "config_incorrect_values.json",
        "config_invalid_json.json",
        "config_missing_keys.json",
        "config_valid.json",
    ]


############ get_full_path ############
# 4 valid case


def test_get_full_path_empty_valid():
    """
    Test case for the get_full_path function when the parameter `root` and `file_name` is empty
    """
    assert get_full_path("", "") == ""


def test_get_full_path_only_root_valid():
    """
    Test case for the get_full_path function when the parameter `root` and `file_name` is valid
    """
    result = get_full_path("/Users/sidd/Documents/github/gos/tests/templates", "")
    assert result == "/Users/sidd/Documents/github/gos/tests/templates/"


def test_get_full_path_only_file_name_valid():
    """
    Test case for the get_full_path function when the parameter `root` and `file_name` is valid
    """
    result = get_full_path("", "test_tree_helper.py")
    assert result == "test_tree_helper.py"


def test_get_full_path_both_valid():
    """
    Test case for the get_full_path function when the parameter `root` and `file_name` is valid
    """
    result = get_full_path(
        "/Users/sidd/Documents/github/gos/tests/templates", "test_tree_helper.py"
    )
    assert (
        result == "/Users/sidd/Documents/github/gos/tests/templates/test_tree_helper.py"
    )