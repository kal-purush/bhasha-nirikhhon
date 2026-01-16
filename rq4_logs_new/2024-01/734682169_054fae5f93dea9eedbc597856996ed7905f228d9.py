"""Download filters from public repository."""

import os
import shutil
import requests


def download_test_filters(url):
    """Download filters."""

    r = requests.get(url, allow_redirects=True, timeout=10)
    with open("filter/test_filters.zip", "wb") as f:
        f.write(r.content)

    shutil.unpack_archive(
        "filter/test_filters.zip",
        "filter/temp",
        "zip",
    )

    shutil.copytree(
        "filter/temp/test_filters",
        "filter",
        dirs_exist_ok=True,
    )

    shutil.rmtree("filter/temp")

    os.remove("filter/test_filters.zip")