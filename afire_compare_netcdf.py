#!/usr/bin/env python
# encoding: utf-8
# Copyright (C) 2017 Space Science and Engineering Center (SSEC),
#  University of Wisconsin-Madison.
#
#     Written by David Hoese    July 2017
#     University of Wisconsin-Madison
#     Space Science and Engineering Center
#     1225 West Dayton Street
#     Madison, WI  53706
#     david.hoese@ssec.wisc.edu
"""Script for comparing netcdf variable data.

Note this is a modified version of the `polar2grid.compare` script.
"""

import sys
import logging
import numpy

LOG = logging.getLogger(__name__)


def compare_array(array1, array2, atol=1e-08, rtol=1e-05):
    """Compare 2 binary arrays per pixel

    Two pixels are considered different if the absolute value of their
    difference is greater than 1. This function assumes the arrays are
    in useful data types, which may cause erroneous results. For example,
    if both arrays are unsigned integers and the different results in a
    negative value overflow will occur and the threshold will likely not
    be met.

    :arg array1:        numpy array for comparison
    :arg array2:        numpy array for comparison
    :keyword threshold: float threshold

    :returns: number of different pixels
    """
    if array1.shape != array2.shape:
        LOG.error("Data shapes were not equal")
        raise ValueError("Data shapes were not equal")

    total_pixels = array1.size
    equal_pixels = numpy.count_nonzero(numpy.isclose(array1, array2, atol=atol, rtol=rtol, equal_nan=True))
    diff_pixels = total_pixels - equal_pixels
    if diff_pixels != 0:
        LOG.warning("%d pixels out of %d pixels are different" % (diff_pixels, total_pixels))
    else:
        LOG.info("%d pixels out of %d pixels are different" % (diff_pixels, total_pixels))

    return diff_pixels


def compare_netcdf(nc1_name, nc2_name, variables, atol, rtol, **kwargs):
    from netCDF4 import Dataset
    nc1 = Dataset(nc1_name, "r")
    nc2 = Dataset(nc2_name, "r")
    num_diff = 0
    for v in variables:
        image1_var = nc1[v]
        image2_var = nc2[v]
        image1_var.set_auto_maskandscale(False)
        image2_var.set_auto_maskandscale(False)
        LOG.debug("Comparing data for variable '{}'".format(v))
        num_diff += compare_array(image1_var, image2_var, atol=atol, rtol=rtol)
    return num_diff


def main(argv=sys.argv[1:]):
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Compare two files per pixel")
    parser.add_argument('-v', '--verbose', dest='verbosity', action="count", default=0,
                        help='each occurrence increases verbosity 1 level through ERROR-WARNING-INFO-DEBUG (default INFO)')
    parser.add_argument('--atol', type=float, default=1.,
                        help="specify absolute tolerance for comparison differences (see numpy.isclose 'atol' parameter)")
    parser.add_argument('--rtol', type=float, default=0.,
                        help="specify relative tolerance for comparison (see numpy.isclose 'rtol' parameter)")
    parser.add_argument('--variables', nargs='+', required=True,
                        help='NetCDF variables to read and compare')
    parser.add_argument('-a', '--a-files', nargs='+', required=True,
                        help="filenames of the first set of files to compare")
    parser.add_argument('-b', '--b-files', nargs='+', required=True,
                        help="filename of the second file to compare")
    args = parser.parse_args(argv)

    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(3, args.verbosity)])
    num_diff = 0
    for file1, file2 in zip(args.a_files, args.b_files):
        LOG.debug("Comparing '{}' to '{}'".format(file1, file2))
        num_diff += compare_netcdf(file1, file2, atol=args.atol,
                                rtol=args.rtol, variables=args.variables)

    if num_diff == 0:
        print("SUCCESS")
        return 0
    else:
        print("FAILURE")
        return 1

if __name__ == "__main__":
    sys.exit(main())
