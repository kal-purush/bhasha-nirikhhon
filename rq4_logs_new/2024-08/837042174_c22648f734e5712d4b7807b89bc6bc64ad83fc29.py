"""
All functions to help with traversing and creating a search tree
"""

import os


def get_folder_icon(is_last: bool) -> str:
    """Returns either of the two icon depending on if its for a file or a directory"""
    return "└── " if is_last else "├── "


def get_prefix(is_last: bool) -> str:
    """Returns either of the two icons depending on if it's for a file or a directory"""
    return "    " if is_last else "│   "


def get_files_and_directories(path: str) -> list:
    """Returns a list of files and directories in the given path"""
    try:
        return sorted(os.listdir(path))
    except NotADirectoryError:
        return []


def get_full_path(root, file_name):
    """combines the root and file_name to create full directory / file path"""
    return os.path.join(root, file_name)