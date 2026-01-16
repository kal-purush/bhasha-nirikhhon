import glob
import os


def find_files(folder_path):
    """
    Finds all the files in the specified folder
    :param folder_path: The path of the folder
    :return: The list of the files
    """
    files_list = [f for f in os.listdir(folder_path)]
    return files_list