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
import logging
import shutil
import traceback
import fcntl

from utils import create_dir

import GridIP

LOG = logging.getLogger('stage_ancillary')


def get_lwm(afire_options, granule_dict):

    try:
        rc = 0
        geo_rc, subset_rc, granulate_rc, shipout_rc = 0, 0, 0, 0
        rc_dict = {'geo': geo_rc, 'subset': subset_rc, 'granulate': granulate_rc,
                   'shipout': shipout_rc}

        # Construct the name of the land water mask template
        lwm_template_file = os.path.join(afire_options['ancil_dir'],
                                         'NPP_VIIRS_LAND_MASK_NASA_1KM.nc')
        LOG.debug("LWM template file = {}".format(lwm_template_file))

        # Create the required dir in the cache dir
        anc_dir = granule_dict['GMTCO']['dt'].strftime('%Y_%m_%d_%j-%Hh')
        lwm_dir = os.path.join(afire_options['cache_dir'], anc_dir)
        if not os.path.isdir(lwm_dir):
            return 1, rc_dict, None

        # Copy the LWM template file to the cache LWM file for this geolocation
        try:
            lwm_file = os.path.join(lwm_dir, granule_dict['GRLWM']['file'])
            LOG.debug("lwm_file = {}".format(lwm_file))
            shutil.copyfile(lwm_template_file, lwm_file)
        except Exception, err:
            LOG.error("Unable to copy the LWM template file {} to the cache file {}, aborting."
                      .format(lwm_template_file, lwm_file))
            LOG.error(err)
            rc_dict = {'geo': geo_rc, 'subset': subset_rc, 'granulate': granulate_rc,
                       'shipout': shipout_rc}
            return 1, rc_dict, None

        # Get the Land Water Mask object
        className = GridIP.classNames['VIIRS-GridIP-VIIRS-Lwm-Mod-Gran']
        LandWaterMask = getattr(GridIP, className)(granule_dict, afire_options)

        # Get the geolocation
        geo_rc = LandWaterMask.setGeolocationInfo()

        # Subset the gridded data for this ancillary object to cover the required lat/lon range.
        subset_rc = LandWaterMask.subset()

        # Granulate the gridded data in this ancillary object for the current granule...
        granulate_rc = LandWaterMask.granulate()

        # Write the new data to the LWM template file
        shipout_rc = LandWaterMask.shipOutToFile(lwm_file)

        rc = int(bool(geo_rc) or bool(subset_rc) or bool(granulate_rc) or bool(shipout_rc))
        rc_dict = {'geo': geo_rc, 'subset': subset_rc, 'granulate': granulate_rc,
                   'shipout': shipout_rc}

    except Exception:
        LOG.info(traceback.format_exc())
        LOG.warn("General warning for  {}".format(granule_dict['granule_id']))
        raise

    return rc, rc_dict, lwm_file


def get_lwm_flock(afire_options, granule_dict):

    rc = 0

    # Construct the full LWM filepath
    anc_dir = granule_dict['GMTCO']['dt'].strftime('%Y_%m_%d_%j-%Hh')
    lwm_dir = os.path.join(afire_options['cache_dir'], anc_dir)
    lwm_file = os.path.join(lwm_dir, granule_dict['GRLWM']['file'])
    LOG.info("Candidate lwm_file = {}".format(lwm_file))

    # Create the required dir in the cache dir
    lwm_dir = create_dir(lwm_dir)
    if lwm_dir is None:
        return 1, None
    else:
        LOG.info("Successfully created cache dir {}".format(lwm_dir))

    # Open the lock file
    lwm_lock_name = "{}.lock".format(lwm_file)
    LOG.info('Creating the lock file {}...'.format(lwm_lock_name))
    lwm_lock_file = open(lwm_lock_name, "a")

    try:
        # Get the lock
        LOG.info('Getting a lock on {}...'.format(lwm_lock_name))
        fcntl.lockf(lwm_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        LOG.info('Successfully got a lock on {}...'.format(lwm_lock_name))

        # We have the lock, so now create the associated file and populate it, or confirm that we
        # already have it...
        if not os.path.exists(lwm_file):

            # Construct the name of the land water mask template
            lwm_template_file = os.path.join(afire_options['ancil_dir'],
                                             'NPP_VIIRS_LAND_MASK_NASA_1KM.nc')
            LOG.info("LWM template file = {}".format(lwm_template_file))

            # Copy the LWM template file to the cache LWM file for this geolocation
            try:
                shutil.copyfile(lwm_template_file, lwm_file)
            except Exception, err:
                LOG.error("Unable to copy the LWM template file {} to the cache file, aborting."
                          .format(lwm_template_file, lwm_file))
                LOG.info(traceback.format_exc())
                LOG.error(err)
                return 1, None

            # Get the Land Water Mask object
            className = GridIP.classNames['VIIRS-GridIP-VIIRS-Lwm-Mod-Gran']
            LandWaterMask = getattr(GridIP, className)(granule_dict, afire_options)

            # Get the geolocation
            geo_rc = LandWaterMask.setGeolocationInfo()

            # Subset the gridded data for this ancillary object to cover the required lat/lon range.
            subset_rc = LandWaterMask.subset()

            # Granulate the gridded data in this ancillary object for the current granule...
            granulate_rc = LandWaterMask.granulate()

            # Write the new data to the LWM template file
            shipout_rc = LandWaterMask.shipOutToFile(lwm_file)

            # Construct the overall return code
            rc = int(bool(geo_rc) or bool(subset_rc) or bool(granulate_rc) or bool(shipout_rc))

        else:
            LOG.info('... {} is already in the local cache.'.format(granule_dict['GRLWM']['file']))

        # Release the lock
        LOG.info('Releasing the lock on {}...'.format(lwm_lock_name))
        fcntl.lockf(lwm_lock_file, fcntl.LOCK_UN)

        # Close the lock file
        LOG.info('Closing the lock file {}'.format(lwm_lock_name))
        lwm_lock_file.close()

    except IOError:

        LOG.info(traceback.format_exc())
        LOG.warn("Could not get a lock on {}".format(lwm_lock_name))
        LOG.info('Closing the lock file {}'.format(lwm_lock_name))
        lwm_lock_file.close()

    return rc, lwm_file
