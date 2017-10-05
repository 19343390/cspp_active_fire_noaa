#!/bin/bash
# Sets the PATH and python interpreter locations for the CSPP Active Fires package
#
# Copyright 2016, University of Wisconsin Regents.
# Licensed under the GNU GPLv3.

export NOAA_AFIRE_VER=NOAA_AFIRE_V_1_0

# python interpreter including numpy, h5py, pytables, scipy; used by CSPP scripts
export PY=${CSPP_ACTIVE_FIRE_HOME}/vendor/ShellB3/bin/python

# common modules location used by CSPP scripts
export PYTHONPATH=${CSPP_ACTIVE_FIRE_HOME}/scripts

export PATH=${PYTHONPATH}:${PATH}

# insurance
export LD_LIBRARY_PATH=${CSPP_ACTIVE_FIRE_HOME}/vendor/ShellB3/lib64
export LD_LIBRARY_PATH=${CSPP_ACTIVE_FIRE_HOME}/vendor/ShellB3/lib:${LD_LIBRARY_PATH}
export LD_LIBRARY_PATH=${CSPP_ACTIVE_FIRE_HOME}/vendor:${LD_LIBRARY_PATH}

export CSPP_ACTIVE_FIRE_STATIC_DIR=${CSPP_ACTIVE_FIRE_HOME}/static_ancillary

if [ "$CSPP_ACTIVE_FIRE_CACHE_DIR" ]; then
    #echo "CSPP_ACTIVE_FIRE_CACHE_DIR is set to "$CSPP_ACTIVE_FIRE_CACHE_DIR
    export CSPP_ACTIVE_FIRE_CACHE_DIR=${CSPP_ACTIVE_FIRE_CACHE_DIR}
fi
