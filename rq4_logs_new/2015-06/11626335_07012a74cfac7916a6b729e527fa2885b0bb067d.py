#!/usr/bin/env python
"""
Warns if a commit has not been pushed today on github.

Usage:
  github_warn.py username
"""


import datetime
import json
import sys

import perform

try:
    from urllib import urlopen
except ImportError:
    from urllib.request import urlopen

def main():
    url = "https://api.github.com/users/{}/repos".format(sys.argv[1])
    repos = json.loads(urlopen(url).read().decode("utf-8"))

    last_updated_at = sorted(repos, key=lambda x: x['pushed_at'])[-1]['pushed_at']

    time_fmt="%Y-%m-%dT%H:%M:%SZ"
    last_updated_at = datetime.datetime.strptime(last_updated_at, time_fmt).date()

    if last_updated_at >= datetime.date.today():
        # User has made update today
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main()
    else:
        print(__doc__)
