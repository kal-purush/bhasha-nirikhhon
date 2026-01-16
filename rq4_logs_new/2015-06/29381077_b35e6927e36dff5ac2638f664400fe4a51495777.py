#!/usr/bin/python3

# A simple script that runs adt-run, and retries a certain number of times
# if it didn't succeed.
#
# To use: simply call this with all the adt-run arguments afterwards, including
# the adt-run call as well:
#
# ./scripts/run-adt.py adt-run --unbuilt-tree foo -o results .....

import sys
import subprocess
from tempfile import NamedTemporaryFile

# Number of times to run adt-run before giving up.
MAX_RUN_COUNT = 3

# adt-run exit codes we should re-run the adt-run call on:
RETRY_CODES = (16, 20)


def main():
    global MAX_RUN_COUNT
    global RETRY_CODES

    arguments = sys.argv[1:]
    for try_num in MAX_RUN_COUNT:
        with NamedTemporaryFile() as stdout, NamedTemporaryFile() as stderr:
            process = subprocess.Popen(
                arguments,
                stdout=stdout,
                stderr=stderr,
            )
            returncode = process.wait()
            if returncode not in RETRY_CODES:
                sys.exit(returncode)


if __name__ == '__main__':
    main()