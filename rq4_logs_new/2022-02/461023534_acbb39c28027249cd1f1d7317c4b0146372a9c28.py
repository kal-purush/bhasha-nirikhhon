import os
import tempfile
import urllib
from os.path import exists
import zipfile

shp_dir = tempfile.TemporaryDirectory()
os.chdir(shp_dir.name)

shp_dir

tempfile.TemporaryFile()

hi = tempfile.TemporaryFile()
hi.write(b'yo')

hi

fp

    with tempfile.TemporaryFile() as fp:
        fp.write(b'Hello world!')

shp_dir
fp

shp_dir

shp_dir.TemporaryFile()
shp_dir

download_data('46')

shp_data = tempfile.TemporaryDirectory()

def download_data(state_fip_code):
    """
    For a given state (in particular its FIPS code), downloads its census tracts and PUMA shapefiles
    from the Census Bureau. The functions skips the download if the file already has been fetched!

    Args:
        state_fip_code: string representing the state of interest

    Returns:
        Saves .shp files for both PUMA and census tracts within the data directory.
    """

    os.chdir(shp_dir.name)

    tract_file = "tl_2019_" + state_fip_code + "_tract"
    puma_file = "tl_2019_" + state_fip_code + "_puma10"

    if exists(tract_file + ".shp"):
        pass
    else:
        req, _ = urllib.request.urlretrieve(
            "https://www2.census.gov/geo/tiger/TIGER2019/PUMA/" + puma_file + ".zip"
        )
        zipped = zipfile.ZipFile(req, "r")
        zipped.extractall()

        req, _ = urllib.request.urlretrieve(
            "https://www2.census.gov/geo/tiger/TIGER2019/TRACT/" + tract_file + ".zip"
        )
        zipped = zipfile.ZipFile(req, "r")
        zipped.extractall()