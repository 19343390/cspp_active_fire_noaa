#!/bin/bash
# Sets various PATH values for the CSPP Active Fires package
#
# Copyright 2016, University of Wisconsin Regents.
# Licensed under the GNU GPLv3.

if [ -z "${CSPP_ACTIVE_FIRE_HOME}" ]; then
    echo "CSPP_ACTIVE_FIRE_HOME must be set to the path where the CSPP software was installed."
    echo "i.e.: export CSPP_ACTIVE_FIRE_HOME=/home/me/cspp_active_fire_noaa"
    exit 1
fi
export PATH=${PATH}:${CSPP_ACTIVE_FIRE_HOME}/vendor/ShellB3/bin
export PATH=${PATH}:${CSPP_ACTIVE_FIRE_HOME}/bin:${CSPP_ACTIVE_FIRE_HOME}/scripts
