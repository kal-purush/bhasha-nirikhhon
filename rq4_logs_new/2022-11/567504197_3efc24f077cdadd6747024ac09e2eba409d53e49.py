import os
from typing import Optional
import re
import sys


def file_scanner(folder_path: Optional[str] = None,
                 import_set: Optional[set] = None):
    for root, _, files in os.walk(folder_path):
        for file in files:
            if os.path.splitext(file)[1] == ".txt":
                get_import(root + "/" + file, import_set)


def get_import(file_path: Optional[str] = None,
               import_set: Optional[set] = None):
    with open(file_path, "r") as f:
        lines = f.readlines()

    for line in lines:
        if re.search(import_re_pattern, line) is not None:
            line = line.replace("\n", "")
            line = line.replace(",", " ")
            import_set.add(line)


def output(res_set: Optional[set] = None):
    res_dict = {}
    for header in res_set:
        header_list = header.split(" ")
        lib_name = header_list[1]
        if lib_name not in res_dict.keys():
            res_dict[lib_name] = [None]
        if len(header_list) > 2 and header_list[2] != "as":
            api_ref = header_list[3:]
            exists_api = res_dict.get(lib_name)
            if exists_api[0] is None and len(exists_api) == 1:
                res_dict[lib_name] = api_ref
            else:
                api_ref += exists_api
            res_dict.update({lib_name: api_ref})
    return res_dict


if __name__ == '__main__':
    import_re_pattern = re.compile(r"import.")
    result = set()
    file_scanner(sys.argv[1], result)
    res = output(result)
    with open(sys.argv[2], "a") as f:
        for key, value in res.items():
            f.write("---------------------------\n")
            f.write("Lib: " + key + "\n")
            if value[0] is None:
                continue
            f.write("Api:\n")
            for item in value:
                f.write("\t")
                f.write(item)
                f.write("\n")