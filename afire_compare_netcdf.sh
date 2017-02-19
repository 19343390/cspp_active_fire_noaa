#!/bin/bash
#
# afire_compare_netcdf.sh
#
# * DESCRIPTION: Verify CSPP Active Fires EDR test products with known products
#
# Created by Geoff Cureton on 2017-02-18.
# Copyright (c) 2017 University of Wisconsin Regents.
# Licensed under GNU GPLv3.

# Check arguments
if [ $# -ne 2 ]; then
  echo "Usage: iapp_compare_netcdf.sh verification_dir work_dir"
  exit 1
fi

# Get primary and secondary directory names
VERIFY_BASE=$1
WORK_DIR=$2

oops() {
    echo "OOPS: $*"
    echo "FAILURE"
    exit 1
}

if [ ! -d $VERIFY_BASE ]; then
    oops "Verification directory $WORK_DIR does not exist"
fi

if [ ! -d $WORK_DIR ]; then
    oops "Working directory $WORK_DIR does not exist"
fi

# Run tests for each test data directory in the base directory
BAD_COUNT=0
for VFILE in $VERIFY_BASE/AFEDR*.nc; do
    SHORT=$(basename $VERIFY_BAS/$VFILE | cut -d_ -f1-5)
    echo "SHORT = "$SHORT
    WFILE=`find $WORK_DIR -name "$SHORT*" -print`
    echo "WFILE = "$WFILE
    echo "VFILE = "$VFILE
    if [ ! -f $WFILE ]; then
        echo "ERROR: Could not find output file $WFILE"
        BAD_COUNT=$(($BAD_COUNT + 1))
        continue
    fi
    echo "Comparing Fire Mask array in $WFILE to validation file"
    $CSPP_ACTIVE_FIRE_HOME/vendor/ShellB3/bin/python <<EOF
import os
import sys
import numpy
from netCDF4 import Dataset

current_dir = os.getcwd()
print "current_dir = {}".format(current_dir)

nc1_name  = "$VFILE"
nc2_name  = "$WFILE"
threshold = 1

print "nc1_name = {}".format(nc1_name)
print "nc2_name = {}".format(nc2_name)

file1_obj = Dataset(nc1_name, "r")
file2_obj = Dataset(nc2_name, "r")

afire1_obj = file1_obj['/Fire Mask/fire_mask']
afire2_obj = file2_obj['/Fire Mask/fire_mask']

afire1_obj.set_auto_maskandscale(False)
afire2_obj.set_auto_maskandscale(False)

afire1_data = afire1_obj[:].astype(numpy.float)
afire2_data = afire2_obj[:].astype(numpy.float)

file1_obj.close()
file2_obj.close()

print afire1_data

if afire1_data.shape != afire2_data.shape:
    print "ERROR: Data shape for '$WFILE' is not the same as the valid '$VFILE'"
    sys.exit(1)

total_pixels = afire1_data.shape[0] * afire1_data.shape[1]
equal_pixels = len(numpy.nonzero((afire2_data - afire1_data) < threshold)[0])
if equal_pixels != total_pixels:
    print "FAIL: {} pixels out of {} pixels are different".format(total_pixels-equal_pixels,total_pixels)
    sys.exit(2)
print "SUCCESS: {} pixels out of {} pixels are different".format(total_pixels-equal_pixels,total_pixels)

EOF
[ $? -eq 0 ] || BAD_COUNT=$(($BAD_COUNT + 1))
done

if [ $BAD_COUNT -ne 0 ]; then
    "$BAD_COUNT files were found to be unequal"
fi

# End of all tests
echo "All files passed"
echo "SUCCESS"

