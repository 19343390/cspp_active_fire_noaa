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

import log_common
from utils import link_files, create_dir, execution_time, execute_binary_captured_inject_io

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

    return 0, anc_dir

def _granulate_GridIP(inDir,geoDicts,algList,dummy_granule_dict):
    '''Granulates the input gridded static data into the required GridIP granulated datasets.'''

    import GridIP
    import Algorithms
    global sdrEndian 
    global ancEndian 

    ANC_SCRIPTS_PATH = path.join(CSPP_RT_HOME,'viirs')
    ADL_ASC_TEMPLATES = path.join(ANC_SCRIPTS_PATH,'asc_templates')

    # Create a list of algorithm module "pointers"
    algorithms = []
    for alg in algList :
        algName = Algorithms.modules[alg]
        algorithms.append(getattr(Algorithms,algName))

    # Obtain the required GridIP collection shortnames for each algorithm
    collectionShortNames = []
    for alg in algorithms :
        for shortName in alg.GridIP_collectionShortNames :
            LOG.info("Adding %s to the list of required collection short names..." \
                    %(shortName))
            collectionShortNames.append(shortName)

    # Remove duplicate shortNames
    collectionShortNames = list(set(collectionShortNames))
    LOG.info("collectionShortNames = %r" %(collectionShortNames))

    # Create a dict of GridIP class instances, which will handle ingest and granulation.
    GridIP_objects = {}
    for shortName in collectionShortNames :

        className = GridIP.classNames[shortName]
        GridIP_objects[shortName] = getattr(GridIP,className)(inDir=inDir,sdrEndian=sdrEndian)
        LOG.debug("GridIP_objects[%s].blobDatasetName = %r" % (shortName,GridIP_objects[shortName].blobDatasetName))
        
        # Just in case the same GridIP class handles more than one collection short name
        if (np.shape(GridIP_objects[shortName].collectionShortName) != () ):
            LOG.debug("    GridIP_objects[%s].collectionShortName = %r" % (shortName,GridIP_objects[shortName].collectionShortName))
            LOG.debug("    GridIP_objects[%s].xmlName = %r" % (shortName,GridIP_objects[shortName].xmlName))
            GridIP_objects[shortName].collectionShortName = shortName
            GridIP_objects[shortName].xmlName = GridIP_objects[shortName].xmlName[shortName]
            LOG.debug("New GridIP_objects[%s].collectionShortName = %r" % (shortName,GridIP_objects[shortName].collectionShortName))
            LOG.debug("New GridIP_objects[%s].xmlName = %r" % (shortName,GridIP_objects[shortName].xmlName))

    # Loop through the required GridIP datasets and create the blobs.
    granIdKey = lambda x: (x['N_Granule_ID'])
    for dicts in sorted(geoDicts,key=granIdKey):
        for shortName in collectionShortNames :
        
            LOG.info("Processing dataset %s for %s" % (GridIP_objects[shortName].blobDatasetName,shortName))

            # Set the geolocation information in this ancillary object for the current granule...
            GridIP_objects[shortName].setGeolocationInfo(dicts)

            # Subset the gridded data for this ancillary object to cover the required lat/lon range.
            GridIP_objects[shortName].subset()

            # Granulate the gridded data in this ancillary object for the current granule...
            GridIP_objects[shortName].granulate(GridIP_objects)

            # Shipout the granulated data in this ancillary object to a blob/asc pair.
            URID = GridIP_objects[shortName].shipOutToFile()

            # If this granule ID is in the list of dummy IDs, add this URID to the 
            # dummy_granule_dict dictionary.
            N_Granule_ID = dicts['N_Granule_ID']
            if N_Granule_ID in dummy_granule_dict.keys():
                try :
                    dummy_granule_dict[N_Granule_ID][shortName] = None
                except :
                    dummy_granule_dict[N_Granule_ID] = {shortName:None}

                dummy_granule_dict[N_Granule_ID][shortName] = URID
                
    return dummy_granule_dict
