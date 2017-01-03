#!/bin/bash
# Sets various PATH values for the CSPP Active Fires package
#
# Copyright 2016, University of Wisconsin Regents.
# Licensed under the GNU GPLv3.

if [ -z "${CSPP_ACTIVE_FIRE_HOME}" ]; then
    echo "CSPP_ACTIVE_FIRE_HOME must be set to the path where the CSPP software was installed."
    echo "export CSPP_ACTIVE_FIRE_HOME=/home/me/cspp_active_fire_noaa"
    #exit 1
fi
export PATH=${CSPP_ACTIVE_FIRE_HOME}/vendor/ShellB3/bin:${PATH}
export PATH=${CSPP_ACTIVE_FIRE_HOME}/bin:${CSPP_ACTIVE_FIRE_HOME}/scripts:${PATH}
#export PATH=${CSPP_ACTIVE_FIRE_HOME}/bin:${CSPP_ACTIVE_FIRE_HOME}/scripts:${CSPP_ACTIVE_FIRE_HOME}/common:${PATH}
#export PATH=${CSPP_ACTIVE_FIRE_HOME}/common/local/bin:${PATH}

# this must have trailing slash
