import os.path
import shutil
from pathlib import Path
from typing import List

import tomlkit
import tomlkit.items
import typer
from poetry.core.pyproject.toml import PyProjectTOML

app = typer.Typer(pretty_exceptions_show_locals=False)


def __validate_and_get_project_toml(file: Path) -> PyProjectTOML:
    toml = PyProjectTOML(file)
    if not toml.is_poetry_project():
        raise RuntimeError(f"{file.resolve()} is not poetry project")

    return toml


def __get_project_main_package(toml: PyProjectTOML) -> str:
    included_packages = toml.poetry_config["packages"]
    included_package_names = [
        pkg["include"] for pkg in included_packages.unwrap() if "include" in pkg
    ]
    if len(included_package_names) > 0:
        return included_package_names[0]


def __get_project_dependencies(toml: PyProjectTOML) -> tomlkit.items.Table:
    return toml.poetry_config["dependencies"]


def __get_project_dev_dependencies(toml: PyProjectTOML) -> tomlkit.items.Table:
    return toml.poetry_config["dev-dependencies"]


def __get_project_scripts(toml: PyProjectTOML) -> tomlkit.items.Table:
    if "scripts" not in toml.poetry_config:
        toml.poetry_config["scripts"] = tomlkit.table()
    return toml.poetry_config["scripts"]


def __unwrap_project_path_dependencies(toml: PyProjectTOML) -> list:
    dev_dependencies = __get_project_dev_dependencies(toml).unwrap()
    path_dependencies = [
        dev_dependencies[dep]
        for dep in dev_dependencies
        if type(dev_dependencies[dep]) is dict
    ]
    return [dep["path"] for dep in path_dependencies if "path" in dep]


def __add_dependency_to_project(toml: PyProjectTOML, dep: dict):
    dependencies = toml.poetry_config["dependencies"]
    if dep["name"] not in dependencies:
        dependencies[dep["name"]] = dep["version"]


def __add_script_to_project(toml: PyProjectTOML, script: dict):
    scripts = __get_project_scripts(toml)
    if script["name"] not in scripts:
        scripts[script["name"]] = script["value"]


def __include_package_in_project(toml: PyProjectTOML, package: str):
    included_packages = toml.poetry_config["packages"]
    included_package_names = [
        pkg["include"] for pkg in included_packages.unwrap() if "include" in pkg
    ]

    if package not in included_package_names:
        new_package_to_include = tomlkit.inline_table().append(
            tomlkit.item("include"), package
        )
        included_packages.append(new_package_to_include)


@app.command()
def project_version(file: Path):
    toml = __validate_and_get_project_toml(file)
    print(toml.poetry_config.get("version"))


@app.command()
def include_path_dependencies(file: Path):
    copy_operations: List[dict] = []
    project_toml = __validate_and_get_project_toml(file)
    path_dependencies = __unwrap_project_path_dependencies(project_toml)

    for dep_path in path_dependencies:
        dir_name = os.path.dirname(Path(file).resolve())
        dep_project_dir = os.path.join(dir_name, dep_path)
        dep_project_file = os.path.join(dep_project_dir, "pyproject.toml")
        dep_project_path = Path(dep_project_file).resolve()
        if not dep_project_path.exists() or not dep_project_path.is_file():
            raise RuntimeError(f"{dep_project_file} is not a file or does not exist")

        dep_toml_file = __validate_and_get_project_toml(dep_project_path)
        dep_main_package = __get_project_main_package(dep_toml_file)
        if dep_main_package is None:
            continue

        dep_main_package_dir = Path(
            os.path.join(dep_project_dir, dep_main_package)
        ).resolve()
        if not dep_main_package_dir.exists() or not dep_main_package_dir.is_dir():
            raise RuntimeError(
                f"{dep_main_package_dir} is not a directory or does not exist"
            )
        copy_operations.append(dict(src=dep_main_package_dir, dst=dir_name))

        __include_package_in_project(project_toml, dep_main_package)

        dep_dependencies = __get_project_dependencies(dep_toml_file)
        for name in dep_dependencies.keys():
            __add_dependency_to_project(
                project_toml,
                dict(name=name, version=dep_dependencies[name]),
            )

        dep_scripts = __get_project_scripts(dep_toml_file)
        for name in dep_scripts.keys():
            __add_script_to_project(
                project_toml,
                dict(name=name, value=dep_scripts[name]),
            )

    for copy_operation in copy_operations:
        base_name = os.path.basename(copy_operation["src"])
        dst_path = Path(os.path.join(copy_operation["dst"], base_name))
        if dst_path.exists():
            shutil.rmtree(dst_path)

        shutil.copytree(copy_operation["src"], dst_path)

    project_toml.save()


if __name__ == "__main__":
    app()