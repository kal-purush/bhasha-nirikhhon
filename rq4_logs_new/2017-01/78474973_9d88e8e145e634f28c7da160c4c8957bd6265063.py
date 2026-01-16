#!/usr/bin/env python

import os
import os.path
import re
import sys
import time


def age(path, max_age=None):
    """Age of file or dir path in days, truncated at max_age."""
    age = int((now - os.path.getmtime(path)) / 86400)   # days
    if (max_age and age > max_age):
        age = max_age
    return(age)


def scan_dir(root):
    # Get agee of each dir as lowest of dir age and age of files contained
    dir_ages = {}
    for path, dirs, files in os.walk(root, topdown=False):
        path_age = age(path, max_age)
        for filename in files:
            if (filename in ignore_files):
                continue
            filename_age = age(os.path.join(path, filename), max_age)
            if (filename_age<path_age):
                path_age = filename_age
        rel_path = os.path.relpath(path, root)
        dir_ages[rel_path] = path_age
        print("# %s %d" % (rel_path, path_age))
    return(dir_ages)


now = time.time()
max_age = 365
ignore_files = [ 'Thumbs.db' ]
for path in sys.argv[1:]:
    print("Scanning %s" % (path))
    dir_ages = scan_dir(path)