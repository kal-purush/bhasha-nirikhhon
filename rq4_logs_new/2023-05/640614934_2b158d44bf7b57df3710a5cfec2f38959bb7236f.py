"""
    Import os
"""
import os


def save_result_to_file(function):
    """
    :param function: *args, **kwargs
    :return: wrapper
    """
    def wrapper(*args, **kwargs):
        result = function(*args, **kwargs)

        class_name = args[0].__class__.__name__
        method_name = function.__name__
        filename = f"{class_name}_{method_name}.txt"

        if os.path.exists(filename):
            file_mode = 'a'
            with open(filename, file_mode) as file:
                if result is not None:
                    file.write(str(result) + '\n')
        else:
            file_mode = 'w'
            with open(filename, file_mode) as file:
                if result is not None:
                    file.write(str(result) + '\n')

        return result

    return wrapper