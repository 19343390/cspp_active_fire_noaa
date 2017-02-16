#!/usr/bin/env python
# encoding: utf-8
"""
stage_ancillary.py

 * DESCRIPTION: 

Created by Geoff Cureton on 2017-02-11.
Copyright (c) 2017 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
import string
import sys
import logging
import time
import types
import fileinput
import shutil
import traceback
from subprocess import Popen, CalledProcessError, call, PIPE
from datetime import datetime

#import netCDF4
from netCDF4 import Dataset, Variable
from netCDF4 import num2date

import log_common
from utils import link_files, create_dir, execution_time, execute_binary_captured_inject_io

import GridIP

LOG = logging.getLogger('stage_ancillary')


def get_lwm(afire_options, granule_dict):

    anc_dir = granule_dict['GMTCO']['dt'].strftime('%Y_%m_%d_%j-%Hh')
    LOG.info("We have set anc_dir['{}'] = {}".format(granule_dict['granule_id'], anc_dir))

    LOG.info("ancil_dir = {}".format(afire_options['ancil_dir']))
    lwm_template_file = os.path.join(afire_options['ancil_dir'], 'NPP_VIIRS_LAND_MASK_NASA_1KM.nc')
    LOG.info("lwm_template_file = {}".format(lwm_template_file))

    LOG.info("cache_dir = {}".format(afire_options['cache_dir']))
    lwm_dir = os.path.join(afire_options['cache_dir'], anc_dir)
    LOG.info("lwm_dir = {}".format(lwm_dir))
    create_dir(lwm_dir)

    lwm_file = os.path.join(lwm_dir, granule_dict['GRLWM']['file'])
    LOG.info("lwm_file = {}".format(lwm_file))

    shutil.copyfile(lwm_template_file, lwm_file)

    # Get the Land Water Mask object
    className = GridIP.classNames['VIIRS-GridIP-VIIRS-Lwm-Mod-Gran']
    LandWaterMask = getattr(GridIP,className)(granule_dict, afire_options)

    # Get the geolocation
    LandWaterMask.setGeolocationInfo()

    # Subset the gridded data for this ancillary object to cover the required lat/lon range.
    LandWaterMask.subset()

    # Granulate the gridded data in this ancillary object for the current granule...
    LandWaterMask.granulate()

    # Write the new data to the LWM template file
    LandWaterMask.shipOutToFile(lwm_file)
    
    return 0, anc_dir
