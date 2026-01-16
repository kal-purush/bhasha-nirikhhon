# Copyright © LFV

import os

from cleo.io.io import IO
from poetry.plugins.plugin import Plugin
from poetry.poetry import Poetry
from reqstool_python_decorators.processors.decorator_processor import ProcessDecorator


class DecoratorsPlugin(Plugin):
    def activate(self, poetry: Poetry, io: IO):
        io.write_line("INSIDE ACTIVATE IN DECORATORSPLUGIN")

        pythonpath_from_pyproject_toml = (
            poetry.pyproject.data.get("tool").get("pytest").get("ini_options").get("pythonpath")
        )

        filtered_pythonpaths = [path for path in pythonpath_from_pyproject_toml if path != "."]
        create_dist_folder_if_not_exist()
        generate_yaml_from_process_decorator(paths=filtered_pythonpaths)


def generate_yaml_from_process_decorator(paths):
    process_decorator = ProcessDecorator()
    process_decorator.process_decorated_data(path_to_python_files=paths)


def create_dist_folder_if_not_exist():
    dist_folder = "dist"

    if not os.path.exists(dist_folder):
        os.makedirs(dist_folder)