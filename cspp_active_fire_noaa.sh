#!/bin/bash
# Bash front-end script for CSPP Active Fires python scripting
#
# Copyright 2016, University of Wisconsin Regents.
# Licensed under the GNU GPLv3.

if [ -z "$CSPP_ACTIVE_FIRE_HOME" ]; then
    echo "CSPP_ACTIVE_FIRE_HOME must be set to the path where the CSPP software was installed."
    echo "export CSPP_ACTIVE_FIRE_HOME=/home/me/cspp_active_fire_noaa"
    exit 1
fi

. ${CSPP_ACTIVE_FIRE_HOME}/cspp_active_fire_noaa_runtime.sh

usage() {
    $PY $CSPP_ACTIVE_FIRE_HOME/scripts/cspp_active_fire_noaa.py --help
}

if [ -z "$1" ]; then
    usage
    exit 3
fi

$PY ${CSPP_ACTIVE_FIRE_HOME}/scripts/cspp_active_fire_noaa.py "$@"

