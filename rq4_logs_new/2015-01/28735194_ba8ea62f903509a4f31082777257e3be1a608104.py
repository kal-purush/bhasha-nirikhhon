from __future__ import print_function

import json
import sys
import os
import subprocess

NUM_DIRS = 82
downloader = "downloader.py"

# Run the downloader client
sp = subprocess.Popen(["python", downloader])
sp.wait()

stories = []
for i in range(1, NUM_DIRS+1):
    with open("karchive/{:02d}.json".format(i), "r") as f:
        items = json.loads(f.read())
        for item in items:
            item["directory"] = i
            stories.append(item)

print([story["title"] + " " + story["url"] for story in stories if "slut" in story["title"].lower()])
print(len(stories))