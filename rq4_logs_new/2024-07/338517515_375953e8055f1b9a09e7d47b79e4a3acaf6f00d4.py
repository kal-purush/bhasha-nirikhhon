
import numpy as np
import netCDF4 as nc
import argparse

# Simple utility to diff pixel files, to understand changes
# per yearly releases...

# Authors: Darren Engwirda

if (__name__ == "__main__"):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        "--base-file", dest="base_file", type=str,
        required=True, 
        default="", help="Pix. file: DIFF=TEST-BASE.")

    parser.add_argument(
        "--test-file", dest="test_file", type=str,
        required=True, 
        default="", help="Pix. file: DIFF=TEST-BASE.")

    parser.add_argument(
        "--diff-file", dest="diff_file", type=str,
        required=True, 
        default="", help="Pix. file: DIFF=TEST-BASE.")

    args = parser.parse_args()

    dat1 = nc.Dataset(args.base_file, "r", format="NETCDF4")
    dat2 = nc.Dataset(args.test_file, "r", format="NETCDF4")

    data = nc.Dataset(args.diff_file, "w", format="NETCDF4")
    
    if ("num_lon" not in data.dimensions.keys()):
        data.createDimension(
            "num_lon", int(dat1.dimensions["num_lon"].size))
    if ("num_lat" not in data.dimensions.keys()):
        data.createDimension(
            "num_lat", int(dat1.dimensions["num_lat"].size))

    if ("num_row" not in data.dimensions.keys()):
        data.createDimension(
            "num_row", int(dat1.dimensions["num_row"].size))
    if ("num_col" not in data.dimensions.keys()):
        data.createDimension(
            "num_col", int(dat1.dimensions["num_col"].size))

    if ("lon" not in data.variables.keys()):
        data.createVariable("lon", "f8", ("num_lon"))
    if ("lat" not in data.variables.keys()):
        data.createVariable("lat", "f8", ("num_lat"))

    data["lon"][:] = np.array(dat1["lon"])
    data["lat"][:] = np.array(dat1["lat"])

    if ("bed_elevation" not in data.variables.keys()):
        data.createVariable(
            "bed_elevation", "i2", ("num_row", "num_col"))

    data["bed_elevation"][:, :] = np.array(dat2["bed_elevation"]) - \
                                  np.array(dat1["bed_elevation"])

    data.close()
