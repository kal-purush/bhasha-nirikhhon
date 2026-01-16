import json
import shutil
import urllib
import zipfile
from functools import lru_cache
from os.path import exists
from pathlib import Path
from typing import Dict, List, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import us
import wquantiles

pd.options.mode.chained_assignment = None  # default='warn'


FilenameType = Union[str, Path]

## things I want to implement:
"""
2. try to infer invalid data? maybe try to implemet with ipumspy
3. better integration over all
"""

ALBERS_EPSG_ID = 5070

# hud_crosswalk = pd.read_excel("resources/ZIP_TRACT_122021.xlsx")


@lru_cache(maxsize=100)
def get_state_intersections(state_fips_code: str) -> gpd.GeoDataFrame:
    puma = gpd.read_file("data/tl_2019_" + state_fips_code + "_puma10.shp").to_crs(
        epsg=ALBERS_EPSG_ID
    )
    tract = gpd.read_file("data/tl_2019_" + state_fips_code + "_tract.shp").to_crs(
        epsg=ALBERS_EPSG_ID
    )

    return gpd.overlay(puma, tract, how="intersection", keep_geom_type=False)


def download_file(url: str, output_filename: FilenameType):
    """
    Download the URL to `output_filename`
    """
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(output_filename, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)


def get_fips_code_from_abbr(state: str) -> str:
    """
    Given a state postal abbreviation, e.g., "RI", return its FIPS code, e.g., "44"

    Args:
        state: The abbreviation of the state

    Returns:
        The FIPS code of the state

    Raises:
    """
    obj = us.states.lookup(state)
    if not obj:
        raise KeyError(f"No such abbreviation: {state}")

    return obj.fips


# var_of_interest, null_val, type
def data_by_zip(zips: List[str], acs_data, variables):
    """
    This function takes in a list of ZIP codes and an ACS datafile and produces summary statistics
    for a given variable. It returns a dictionary containing the summary for each ZIP code.

    Args:
        zips: a list of zip codes, presented as strings (i.e. ['02835', '79901'])
        acs_data: either a pd.DataFrame of the ACS data OR a string of its filepath
        var_of_interest: the variable of interest as a string (i.e. 'HHINCOME')
        null_val: what defines a null entry. Often 9999999 or 0, check IPUMS documentation.
        type: whether the variable of interest is 'household' or 'individual' data
    """

    ans_dict = {}

    # TODO(khw): zip is a keyword, change the name
    for zip in zips:
        tracts, state_fips_code = zip_to_tract(zip)

        if sum([x[1] for x in tracts]) < 1e-7:
            raise ValueError(f"{zip} is not a valid residential zip code!")

        download_data(state_fips_code)
        puma_ratios = tracts_to_puma(tracts, state_fips_code)

        ans = get_acs_data(acs_data, variables, int(state_fips_code), puma_ratios)
        ans_dict[zip] = ans

    return ans_dict


def download_data(state_fip_code):
    """
    For a given state (in particular its FIPS code), downloads its census tracts and PUMA shapefiles
    from the Census Bureau. The functions skips the download if the file already has been fetched!

    Args:
        state_fip_code: string representing the state of interest

    Returns:
        Saves .shp files for both PUMA and census tracts within the data directory.
    """
    tract_file = "tl_2019_" + state_fip_code + "_tract"
    puma_file = "tl_2019_" + state_fip_code + "_puma10"

    if exists("data/" + tract_file + ".shp"):
        pass
    else:
        req, _ = urllib.request.urlretrieve(
            "https://www2.census.gov/geo/tiger/TIGER2019/PUMA/" + puma_file + ".zip"
        )
        zipped = zipfile.ZipFile(req, "r")
        zipped.extractall(path="data/")

        req, _ = urllib.request.urlretrieve(
            "https://www2.census.gov/geo/tiger/TIGER2019/TRACT/" + tract_file + ".zip"
        )
        zipped = zipfile.ZipFile(req, "r")
        zipped.extractall(path="data/")


def zip_to_tract(zip):
    """
    For a given ZIP code, uses HUD Crosswalk data (https://www.huduser.gov/portal/datasets/usps_crosswalk.html)
    to find the ratio of persons in each census tract for the given ZIP code.

    Args:
        zip: the five-digit ZIP code of interest, written as a string

    Returns:
        List containing the same number of entries as census tracts within the ZIP code. Each entry is a list,
        entry 0 is the census tact and entry 1 is the residential ratio of the census tract within that ZIP.
    """
    try:
        zip_str = zip
        zip = int(zip)
    except ValueError:
        pass

    contained = hud_crosswalk[hud_crosswalk["zip"] == zip][["tract", "res_ratio"]]
    try:
        state = hud_crosswalk[hud_crosswalk["zip"] == zip]["usps_zip_pref_state"].iloc[
            0
        ]
    except IndexError:
        raise KeyError(zip_str + " is not a valid ZIP code!")
    return contained.values.tolist(), get_fips_code_from_abbr(state)


def tracts_to_puma(tracts, state_fip_code: str):
    """
    Takes in a list of tracts and ratios for a given zip code (in a given state) and returns the PUMAs
    composing the ZIP code with ratios (i.e. 88% in PUMA 00101 and 12% in PUMA 00102).

    Args:
        tracts: a 2D list generated by zip_to_tract containing census tracts and weighted_ratios
        state_fip_code: string representing state of interest

    Returns:
        series containing ratio of population for each PUMA
    """
    intersection_gdf = get_state_intersections(state_fip_code)
    intersection_gdf["shape_area"] = intersection_gdf.area
    intersection_gdf["GEOID"] = intersection_gdf["GEOID"].astype("int")

    # TODO(jn): Make this comprehensible
    grouped_gdf = intersection_gdf.groupby("GEOID").agg(lambda x: list(x))[
        ["PUMACE10", "shape_area"]
    ]
    grouped_gdf["ratios"] = grouped_gdf["shape_area"].apply(
        lambda x: [y / sum(x) for y in x]
    )

    sorted = grouped_gdf.loc[[int(x[0]) for x in tracts]][
        ["PUMACE10", "ratios"]
    ].explode(column=["PUMACE10", "ratios"])
    sorted["ratios"] = sorted["ratios"].apply(lambda x: 1 if x > 0.99 else x)
    out = sorted[sorted["ratios"] > (1 - 0.99)]
    joined = out.join(
        pd.DataFrame(tracts).astype({0: "int", 1: "float32"}).set_index(0)
    )
    joined["weighted_ratios"] = joined["ratios"] * joined[1]
    return joined.groupby("PUMACE10").sum()["weighted_ratios"]


# variables, null_val, type,
def get_acs_data(
    file: Union[FilenameType, pd.DataFrame],
    variables: Dict[str, str],
    state_fip_code,
    pumas,
):
    if isinstance(file, (str, Path)):
        data = pd.read_csv(file)
    elif isinstance(file, pd.DataFrame):
        data = file

    sub_state = data[data["STATEFIP"] == state_fip_code]
    # TODO(jn): Why is the cast to float necessary
    sub_state["HHWT"] = sub_state["HHWT"].astype(float)
    sub_state["PERWT"] = sub_state["PERWT"].astype(float)
    grouped = sub_state.groupby("PUMA")

    outer_dict = {}

    for entry in variables:

        variable = entry
        null_val = variables[variable]["null"]
        var_type = variables[variable]["type"]

        avg_list = []
        median_list = []
        std_list = []

        # TODO(jn) something like this to DRY this out
        # rel_df = df if var_type = "individual" else df[df["PERNUM"] == 1]
        for index, i in enumerate(pumas):
            this_puma = grouped.get_group(int(pumas.index[index]))

            if var_type == "household":
                this_puma[variable] = this_puma[variable].astype(float)
                this_puma = this_puma[this_puma["PERNUM"] == 1]
                this_puma = this_puma[this_puma[variable] != null_val]

                this_puma["weighted"] = this_puma[variable] * this_puma["HHWT"]
                avg = this_puma["weighted"].sum() / this_puma["HHWT"].sum()
                avg_list.append(avg * i)

                median = wquantiles.median(this_puma[variable], this_puma["HHWT"])
                median_list.append(median * i)

                std = np.sqrt(
                    (((this_puma[variable] - avg) ** 2) * this_puma["HHWT"]).sum()
                    / (
                        ((len(this_puma["HHWT"]) - 1) / len(this_puma["HHWT"]))
                        * this_puma["HHWT"].sum()
                    )
                )
                std_list.append(std * i)
            elif var_type == "individual":
                this_puma[variable] = this_puma[variable].astype("float")
                this_puma["weighted"] = this_puma[variable] * this_puma["PERWT"]
                avg = this_puma["weighted"].sum() / this_puma["PERWT"].sum()
                avg_list.append(avg * i)

                median = wquantiles.median(this_puma[variable], this_puma["PERWT"])
                median_list.append(median * i)

                std = np.sqrt(
                    (((this_puma[variable] - avg) ** 2) * this_puma["PERWT"]).sum()
                    / (
                        ((len(this_puma["PERWT"]) - 1) / len(this_puma["PERWT"]))
                        * this_puma["PERWT"].sum()
                    )
                )
                std_list.append(std * i)

            else:
                raise ValueError(f"{var_type} is not a valid type was specified!")

        outer_dict[variable] = {
            "mean": round(sum(avg_list), 2),
            "std": round(sum(std_list), 2),
            "median": round(sum(median_list), 2),
        }

    return outer_dict