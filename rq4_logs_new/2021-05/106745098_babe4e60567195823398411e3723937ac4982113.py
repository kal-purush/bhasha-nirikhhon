"""
Contains functions related to the calculation of emod3d subdomain boundaries.
Functions ported from C contain a number of calls to np.int32 and np.float32 calls to emulate single precision integer and floating point behaviour.
Code ported from emod3d v3.0.8 misc.c. This is consistent with v3.0.7. 
While v3.0.4 uses long doubles in place of floats, this does not seem to practically increase the accuracy of calculation.
"""

import argparse

import numpy as np


def get_start_boundary(n_grid_points, n_subdomains, index_subdomain):
    """
    Calculates the starting boundary of the subdomain for a given subdomain index along a velocity model axis
    Should have an overlap of 4 with the previous subdomains ending boundary
    Does not account for the first subdomain
    :param n_grid_points: The number of grid points along the axis
    :param n_subdomains: The number of subdomains along the axis
    :param index_subdomain: The index of the subdomain being tested. May be an integer or array of integers
    :return: The first grid point(s) covered by the given subdomain index(cies)
    """
    fslice = np.float32(
        np.float32(n_grid_points + (n_subdomains - 1.0) * 4.0) / np.float32(n_subdomains) - 1.0
    )
    fn1 = np.float32(index_subdomain * (fslice - 3.0))
    nx1 = np.int32(fn1 + 0.5)
    return nx1


def get_end_boundary(n_grid_points, n_subdomains, index_subdomain):
    """
    Calculates the ending boundary of the subdomain for a given subdomain index along a velocity model axis
    Should have an overlap of 4 with the next subdomains starting boundary
    Does not account for the last subdomain points
    :param n_grid_points: The number of grid points along the axis
    :param n_subdomains: The number of subdomains along the axis
    :param index_subdomain: The index of the subdomain being tested. May be an integer or array of integers
    :return: The last grid point(s) covered by the given subdomain index(cies)
    """
    fslice = np.float32(
        np.float32(n_grid_points + (n_subdomains - 1.0) * 4.0) / np.float32(n_subdomains) - 1.0
    )
    fn1 = np.float32(index_subdomain * (fslice - 3.0))
    fn1 = np.float32(fn1 + fslice)
    nx2 = np.int32(fn1 + 0.5)
    nx2 = np.int32(nx2 + 1)
    return nx2


def get_nproc(
    nproc: np.int32,
    globnx: np.int32,
    globny: np.int32,
    globnz: np.int32,
    min_nproc: np.int32 = np.int32(1),
    nproc_x: np.int32 = np.int32(-1),
    nproc_z: np.int32 = np.int32(-1),
):
    """
    Ported from the source of emod3d. Casting enforces C like behaviour.
    Calculates the number of processes to use along each axis of a velocity model.
    The argument min_nproc sets the unit size, allowing for blocks of grid points to be assigned to subdomains, instead of individual points
    The nproc_x/z argument are available to mimic options available in the C. Not normally used
    :param nproc: The number of processes to be used.
    :param globnx: The number of velocity model grid points along the x axis.
    :param globny: The number of velocity model grid points along the y axis.
    :param globnz: The number of velocity model grid points along the z axis.
    :param min_nproc: Multiplyer to perform calculations using cubes of min_nproc, defaults to 1.
    :param nproc_x: The number of processes to use in the x direction. Set value above -1 to specify the number to use. Defaults to -1.
    :param nproc_z: The number of processes to use in the y direction. Set value above -1 to specify the number to use. Defaults to -1.
    :return: A tuple containing:
        The number of processes along the x axis
        The number of processes along the y axis
        The number of processes along the z axis
    """
    inv_fmp = np.float32(1.0 / min_nproc)
    fnp = np.float32(nproc)
    fnx = np.float32(globnx)
    fny = np.float32(globny)
    fnz = np.float32(globnz)

    if nproc_z < 0:
        nproc_z = np.int32(
            inv_fmp * fnz * np.exp(np.log(fnp / (fnx * fny * fnz)) / 3.0) + 0.5
        )
        if nproc_z < 1:
            nproc_z = np.int32(1)
        nproc_z = np.int32(min_nproc * nproc_z)

    if nproc_x < 0:
        nproc_x = np.int32(
            inv_fmp * fnx * np.exp(np.log(fnp / (fnx * fny * nproc_z)) / 2.0) + 0.5
        )
        if nproc_x < 1:
            nproc_x = np.int32(1)
        nproc_x = np.int32(min_nproc * nproc_x)

    nproc_y = np.int32(
        inv_fmp * fnp / (np.float32(nproc_x) * np.float32(nproc_z)) + 0.5
    )
    if nproc_y < 1:
        nproc_y = np.int32(1)
    nproc_y = np.int32(min_nproc * nproc_y)

    nproc_c = nproc_x * nproc_y * nproc_z

    if nproc_c != nproc:
        # Alternate method of calculating the processes distribution
        ip3 = np.int32(np.exp(np.log(fnp) / 3.0) + 0.5)

        ipt = np.int32(1)
        while 2 * ipt <= ip3 and nproc % ipt == 0 and (nproc / ipt) % 2 == 0:
            ipt = np.int32(2 * ipt)

        nproc_z = ipt

        np2 = np.int32(nproc / nproc_z)
        ip2 = np.int32(np.exp(np.log(1.0 * np2) / 2.0) + 0.5)

        ipt = np.int32(1)
        while 2 * ipt <= ip2 and np2 % ipt == 0 and (np2 / ipt) % 2 == 0:
            ipt = np.int32(2 * ipt)

        nproc_x = np.int32(ipt)
        nproc_y = np.int32(np2 / nproc_x)

    return nproc_x, nproc_y, nproc_z


def test_domain(nx, ny, nz, nc):
    """
    Tests a given domain size and core count to check for grid points that won't be assigned to any sub domain
    :param nx: The number of grid points in the x direction
    :param ny: The number of grid points in the y direction
    :param nz: The number of grid points in the z direction
    :param nc: The number of cores to be used to perform the simulation
    :return: Three arrays with the index of any unassigned grid lines. If all three are empty then the simulation will work as expected
    """
    nproc_x, nproc_y, nproc_z = get_nproc(nc, nx, ny, nz)

    x_indicies = np.arange(nproc_x - 1)
    x_n1 = get_start_boundary(nx, nproc_x, x_indicies + 1)
    x_n2 = get_end_boundary(nx, nproc_x, x_indicies)

    y_indicies = np.arange(nproc_y - 1)
    y_n1 = get_start_boundary(ny, nproc_y, y_indicies + 1)
    y_n2 = get_end_boundary(ny, nproc_y, y_indicies)

    z_indicies = np.arange(nproc_z - 1)
    z_n1 = get_start_boundary(nz, nproc_z, z_indicies + 1)
    z_n2 = get_end_boundary(nz, nproc_z, z_indicies)

    x_mask = np.where(x_n1 + 2 != x_n2 - 2)[0]
    y_mask = np.where(y_n1 + 2 != y_n2 - 2)[0]
    z_mask = np.where(z_n1 + 2 != z_n2 - 2)[0]

    return x_mask, y_mask, z_mask


def load_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "n_cores", type=int, help="The number of cores used to perform the simulation"
    )
    parser.add_argument(
        "nx", type=int, help="The number of grid points along the x axis"
    )
    parser.add_argument(
        "ny", type=int, help="The number of grid points along the y axis"
    )
    parser.add_argument(
        "nz", type=int, help="The number of grid points along the z axis"
    )
    return parser.parse_args()


def main():
    """
    Uses the command line arguments provided to determine if the simulation will have any grid lines that are not associated with a subdomain.
    If any are not associated they are printed to stdout, and the script exits with an exit code of 1
    Otherwise the script exits with an exit code of 0
    """
    args = load_args()
    x, y, z = test_domain(args.nx, args.ny, args.nz, args.n_cores)

    if x.size + y.size + z.size > 0:
        message_parts = []
        if x.size > 0:
            message_parts.append("Missed x axis indicies:")
            message_parts.append(", ".join(x.astype(str)))
        if y.size > 0:
            message_parts.append("Missed y axis indicies:")
            message_parts.append(", ".join(y.astype(str)))
        if z.size > 0:
            message_parts.append("Missed z axis indicies:")
            message_parts.append(", ".join(z.astype(str)))
        print(". ".join(message_parts))
        return_code = 1
    else:
        return_code = 0
    exit(return_code)


if __name__ == "__main__":
    main()