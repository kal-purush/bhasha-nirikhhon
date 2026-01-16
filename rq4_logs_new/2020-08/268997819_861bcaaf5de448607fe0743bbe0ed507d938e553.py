'''
Utilities around configuring and downloading local files.
'''


import concurrent.futures
import logging
import os
import os.path
from typing import Dict
import shutil
import requests
from typeguard import typechecked
from pyconfigurableml._core import run_with_specified_config


log = logging.getLogger(__name__)


@typechecked
def download_url_to_file(url: str, path: str) -> None:
    '''
    Efficient code for downloading a URL to a file. Will attempt to clean up
    (delete the file) if downloading fails.
    https://stackoverflow.com/a/39217788
    '''
    try:
        with requests.get(url, stream=True) as req:
            with open(path, 'wb') as file:
                shutil.copyfileobj(req.raw, file)

    # Attempt to clean up:
    # https://stackoverflow.com/a/10840586
    except:  # noqa: E722
        log.error(f'Could not download {url} to {path}')
        try:
            os.remove(path)
        except OSError:
            pass

        raise


@typechecked
def download_url_to_file_if_not_exists(url: str, path: str) -> None:
    '''
    Download url to path only if the file at path doesn't already exist.
    '''
    if not os.path.isfile(path):
        log.warning(f'Downloading {url} to {path}')
        download_url_to_file(url, path)


@typechecked
def download_urls_to_files_if_not_exist(path_to_url: Dict[str, str]) -> None:
    '''
    Ensure that all paths in the mapping path -> URL exist, downloading them
    in parallel (max 5 threads) if necessary.

    Follows:
    https://www.digitalocean.com/community/tutorials/how-to-use-threadpoolexecutor-in-python-3
    '''
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for key, value in path_to_url.items():
            futures.append(
                executor.submit(download_url_to_file_if_not_exists, url=value, path=key)
                )

        concurrent.futures.as_completed(futures)


@run_with_specified_config(__name__)
@typechecked
def ensure_files_exist(config, inner_config: Dict[str, str]):
    '''
    Ensure that the files configured by inner_config exist. Does not modify the
    configuration itself.
    '''
    download_urls_to_files_if_not_exist(inner_config)

    return config