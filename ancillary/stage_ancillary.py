#!/usr/bin/env python
# encoding: utf-8
"""
stage_ancillary.py

 * DESCRIPTION: This file granulates any gridded static ancillary for a single granule ID.

Created by Geoff Cureton on 2017-02-11.
Copyright (c) 2017 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import logging
import shutil
import traceback
import fcntl
from subprocess import call, check_call, CalledProcessError

from utils import create_dir, execute_binary_captured_inject_io

import ancillary.GridIP as GridIP

LOG = logging.getLogger('stage_ancillary')

def nc_from_cdl(afire_options, granule_dict, lwm_file):
    '''
    Creates an empty output netCDF4 file from a CDL file.
    '''
    env_vars = {}
    lwm_cdl_file = pjoin(afire_options['ancil_dir'],
                                     'AF-LAND_MASK_NASA_1KM.cdl')

    ncgen_bin = pjoin(afire_options['afire_home'], 'vendor/ShellB3/bin', 'ncgen')
    cmd = '{} -b {} -o {}'.format(ncgen_bin,
                                  lwm_cdl_file,
                                  lwm_file)
    LOG.debug("cmd = {}".format(cmd))

    try:
        LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
        rc = check_call([cmd], shell=True, env={})
        LOG.debug('check_call() return value: {}'.format(rc))
    except CalledProcessError as err:
        rc = err.returncode
        LOG.error("ncgen returned a value of {}".format(rc))
        return rc

    return rc

def get_lwm(afire_options, granule_dict):
    '''
    Generate a granulated Land Water Mask (LWM) from the VIIRS GMTCO geolocation, and a global 0.5 degree
    grid of the Land Water Mask.
    '''

    try:

        rc = 0
        geo_rc, subset_rc, granulate_rc, shipout_rc = 0, 0, 0, 0
        rc_dict = {'geo': geo_rc, 'subset': subset_rc, 'granulate': granulate_rc,
                   'shipout': shipout_rc}

        # Check that the required cache dir already exists...
        #geo_prefix = 'GITCO' if afire_options['i_band'] else 'GMTCO' # FUTURE
        geo_prefix = 'GMTCO'
        anc_dir = granule_dict[geo_prefix]['dt'].strftime('%Y_%m_%d_%j-%Hh')
        lwm_dir = pjoin(afire_options['cache_dir'], anc_dir)
        if not isdir(lwm_dir):
            LOG.error("LWM dir {} is not considered a valid directory, aborting.".format(lwm_dir))
            return 1, rc_dict, None

        # Construct the name of the output GRLWM file
        lwm_file = pjoin(lwm_dir, granule_dict['GRLWM']['file'])
        LOG.debug("Candidate LWM filename: {}".format(lwm_file))

        lwm_required = False

        # Check whether the GRLWM file exists...
        if exists(lwm_file):

            LOG.debug("LWM file {} exists...".format(lwm_file))

            if not isfile(lwm_file):
                LOG.warning("{} is not a regular file, removing...".format(lwm_file))
                os.remove(lwm_file)
                lwm_required = True
            else:
                LOG.debug("{} is a regular file, checking size...".format(lwm_file))
                min_lwm_size_mb = 20.
                lwm_size = float(os.stat(lwm_file).st_size) / (1024. * 1024.)
                LOG.debug("lwm_file {} has size {} Mb".format(lwm_file, lwm_size))
                if lwm_size < min_lwm_size_mb:
                    LOG.warning("Size of lwm_file {} < minimum of 20 Mb".format(lwm_file))
                    os.remove(lwm_file)
                    lwm_required = True
        else:
            LOG.debug("LWM file {} doesn't exist...".format(lwm_file))
            lwm_required = True

        LOG.debug("lwm_required =  {}".format(lwm_required))

        # We need a new LWM file, create it
        if lwm_required:

            # Copy the LWM template file to the cache LWM file for this geolocation
            try:
                _ = nc_from_cdl(afire_options, granule_dict, lwm_file)
                #shutil.copyfile(lwm_template_file, lwm_file)
            except Exception as err:
                LOG.error("Unable to copy the LWM template file {} to the cache file {}, aborting."
                          .format(lwm_template_file, lwm_file))
                LOG.error(err)
                rc_dict = {'geo': geo_rc, 'subset': subset_rc, 'granulate': granulate_rc,
                           'shipout': shipout_rc}
                return 1, rc_dict, None

            # Get the Land Water Mask object
            LandWaterMask = GridIP.LandWaterMask(granule_dict, afire_options)

            # Get the geolocation
            geo_rc = LandWaterMask.setGeolocationInfo()

            # Subset the gridded data for this ancillary object to cover the required lat/lon range.
            subset_rc = LandWaterMask.subset()

            # Granulate the gridded data in this ancillary object for the current granule...
            granulate_rc = LandWaterMask.granulate()

            # Write the new data to the LWM template file
            shipout_rc = LandWaterMask.shipOutToFile(lwm_file, afire_options)

            min_lwm_size_mb = 20.
            lwm_size = float(os.stat(lwm_file).st_size) / (1024. * 1024.)
            LOG.debug("lwm_file {} has size {} Mb".format(lwm_file, lwm_size))
            if lwm_size < min_lwm_size_mb:
                LOG.warning("Size of lwm_file {} < minimum of 20 Mb".format(lwm_file))
                shipout_rc = 1

        else:
            geo_rc, subset_rc, granulate_rc, shipout_rc = 0, 0, 0, 0

        rc = int(bool(geo_rc) or bool(subset_rc) or bool(granulate_rc) or bool(shipout_rc))
        rc_dict = {'geo': geo_rc, 'subset': subset_rc, 'granulate': granulate_rc,
                   'shipout': shipout_rc}

    except Exception:
        LOG.warn(traceback.format_exc())
        LOG.warn("General warning for  {}".format(granule_dict['granule_id']))
        raise

    return rc, rc_dict, lwm_file
