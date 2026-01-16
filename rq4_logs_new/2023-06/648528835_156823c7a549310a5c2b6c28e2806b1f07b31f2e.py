import importlib
import json
import os


def check_module(name, info):
    package = info['package']
    version = info['version']
    try:
        importlib.import_module(name)
    except ImportError:
        print(f'\033[34minstall \033[32m{package}\033[0m')
        os.system(f'pip install {package}=={version}')
        print()


if __name__ == '__main__':
    with open(r'.\res\modules.json', 'r', encoding='utf-8') as file:
        modules_dict = json.load(file)
    for key, value in modules_dict.items():
        check_module(key, value)