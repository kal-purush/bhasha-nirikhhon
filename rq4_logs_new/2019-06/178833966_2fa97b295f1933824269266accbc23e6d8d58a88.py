from zipfile import ZipFile, ZIP_DEFLATED
import os
import sys
import re
import hashlib
import argparse
import json
from pathlib import Path
import base64

parser = argparse.ArgumentParser()
parser.add_argument("output", help="file to output")
parser.add_argument(
    "-x",
    "--exclude_common_ignores",
    help="Toggle excluding common deps e.g. boto3 that need not be packaged",
    action="store_true")
parser.add_argument("config_file", help="Config for packaging")
parser.add_argument("pipenv_lock", help="Pipenv lock file to exclude dev deps")

COMMON_EXCLUDES = [
    "boto3",
    "botocore",
    "docutils",
    "jmespath",
    "python-dateutil",
    "s3transfer",
    "six",
    "pip",
    "setuptools"
]

args = parser.parse_args()


def with_dist_info(excludes):
    return [x for i in excludes for x in (f"^{i}(/|$)", f"^{i}-\\d+(\\.\\d+)*\\.dist-info")]


with open(args.config_file, "r") as config_file, open(args.pipenv_lock, "r") as pipenv_lock:
    pipenv_json = json.load(pipenv_lock)
    pipenv_excludes = set(pipenv_json["develop"].keys()) - set(pipenv_json["default"].keys())
    pipenv_excludes = with_dist_info(pipenv_excludes)
    config = json.load(config_file)
    base_dirs = config["base_dirs"]
    exclude = config["excludes"]

    if not args.exclude_common_ignores:
        exclude = exclude + COMMON_EXCLUDES
    exclude = with_dist_info(exclude)

    exclude = [re.compile(i) for i in exclude]
    home = os.getcwd()

    parent_dir = Path(args.output).parent
    if not parent_dir.exists():
        try:
            os.makedirs(parent_dir)
        except FileExistsError:
            pass

    with open(f"{args.output}.log", "w") as log:
        with ZipFile(args.output, "w", compression=ZIP_DEFLATED) as zip_file:
            for base_dir in base_dirs:
                os.chdir(home)
                base_dir_path = os.path.abspath(base_dir["dir"])

                # on linux need the python version too
                if sys.platform != "win32":
                    base_dir_path = base_dir_path.replace(
                        ".venv/Lib/site-packages", ".venv/lib/python3.7/site-packages")

                os.chdir(base_dir_path)
                for folder, sub_folders, file_names in os.walk(base_dir_path):
                    exclude_here = exclude
                    if "exclude_dev_deps" in base_dir:
                        exclude_here = exclude + pipenv_excludes
                        print(f"Since we are ignoring dev deps, will not package {exclude_here}", file=log)
                    folder_name = str(Path(folder).relative_to(base_dir_path))
                    if any((re.search(x, folder_name) for x in exclude_here)):
                        print(f"Ignoring all files in folder {folder_name}", file=log)
                        sub_folders.clear()
                    else:
                        for file_name in file_names:
                            file = os.path.join(folder, file_name)
                            write_name = str(Path(file).relative_to(base_dir_path))
                            if any((re.search(x, write_name) for x in exclude_here)):
                                print(f"Ignoring file {file}", file=log)
                            else:
                                print(f"Adding file to zip {file}", file=log)
                                if "base_in_zip" in base_dir:
                                    write_name = os.path.join(base_dir["base_in_zip"], write_name)
                                zip_file.write(file, write_name)

    os.chdir(home)

    hasher = hashlib.sha256()
    with open(f"{args.output}", "rb") as zipped_file:
        buf = zipped_file.read()
        hasher.update(buf)

    print(f"{{\"status\":\"ok\", \"hash\": \"{base64.b64encode(hasher.digest()).decode('UTF-8')}\"}}")