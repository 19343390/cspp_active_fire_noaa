#!/usr/bin/env python
# encoding: utf-8
"""
cspp_active_fire_noaa.py

 * DESCRIPTION: This is the main driver script for CSPP NOAA Active-Fire, through which ancillary
 data is transcoded, and 'vfire' is executed on the supplied input files.

Created by Geoff Cureton on 2017-01-03.
Copyright (c) 2017 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
import sys
import re
import string
import shutil
import logging
import time
import glob
import numpy as np
import traceback
import time
import datetime as dt
from datetime import datetime, timedelta
from cffi import FFI
import fcntl

ffi = FFI()
from args import argument_parser
from active_fire_interface import generate_file_list, construct_cmd_invocations
from utils import  setup_cache_dir, clean_cache, link_files, CsppEnvironment
from utils import check_and_convert_path, check_and_convert_env_var, check_existing_env_var

os.environ['TZ'] = 'UTC'

LOG = logging.getLogger(__name__)

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

def process_afire_inputs(afire_home, work_dir, afire_options):
    """
    Take the obtained / created gvar inputs and remap to standard projection
    """

    ret_val = 0

    attempted_runs = []
    successful_runs = []
    crashed_runs = []
    problem_runs = []

    # Create a list of dicts containing valid inputs
    afire_data_dict =  generate_file_list(afire_options['inputs'], afire_options)
    granule_id_list = afire_data_dict.keys()
    granule_id_list.sort()

    # Create a dict containing the required command line invocations.
    afire_data_dict = construct_cmd_invocations(afire_data_dict)

    LOG.info('')
    LOG.info('>>> Input Files')
    LOG.info('')
    for granule_id in granule_id_list:
        for file_kind in ['GMTCO','SVM05','SVM07', 'SVM11', 'SVM13', 'SVM15', 'SVM16']:
            LOG.info('afire_data_dict[{}][{}] = {},{}'.format(
                granule_id,
                file_kind,
                afire_data_dict[granule_id][file_kind]['dt'],
                afire_data_dict[granule_id][file_kind]['file']
                )
            )
        LOG.info('')

    LOG.info('')
    LOG.info('>>> Command Line Invocations')
    LOG.info('')
    for granule_id in granule_id_list:
        LOG.info('granule_id[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['granule_id']))
        LOG.info('dt[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['GMTCO']['dt']))
        LOG.info('anc_dir[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['anc_dir']))
        LOG.info('anc_file[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['GRLWM']['file']))
        LOG.info('run_dir[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['run_dir']))
        LOG.info('cmd[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['cmd']))

    # Clean out product cache files that are too old.
    LOG.info('')
    if not afire_options['preserve_cache']:
        LOG.info(">>> Cleaning the ancillary cache back {} hours...".format(afire_options['cache_window']))
        first_dt = afire_data_dict[granule_id_list[0]]['GMTCO']['dt']
        clean_cache(afire_options['cache_dir'], afire_options['cache_window'], first_dt)

    '''

    if granule_id_list:

        for input_file in input_files:

            LOG.info(">>> Processing input file {}".format(input_file))
            LOG.debug("work_dir = {}".format(work_dir))

            # Set the input file
            afire_options['inputs'] = input_file
            sat_obj.set_input_paths(input_file)

            # Setting the input dir and file basename
            input_dir = sat_obj.input_dir
            input_file = sat_obj.input_filename

            LOG.info("input_dir = {}".format(input_dir))
            LOG.info("input_file = {}".format(input_file))

            attempted_runs.append(input_file)

            # Create the run dir for this input file
            log_idx = 0
            while True:
                run_dir = os.path.join(work_dir,"geocat_{}_run_{}".format(input_file,log_idx))
                if not os.path.exists(run_dir):
                    os.makedirs(run_dir)
                    break
                else:
                    log_idx += 1

            # Determine the datetime object for this input file...
            LOG.debug("getting the datetime object for {}".format(input_file))
            input_dt = sat_obj.datetime_from_input_filename(input_file)
            LOG.debug("datetime object for {} is : {}".format(input_file,input_dt))

            # Clean out product cache files that are too old.
            if not afire_options['preserve_cache']:
                LOG.info("Cleaning the temporal cache back {} hours...".format(afire_options['cache_window']))
                clean_hdf(afire_options['tmp_dir'],afire_options['cache_window'],input_dt)


            # Download and stage the required ancillary data for this input file
            LOG.info("Staging the required ancillary data...")
            rc_ancil = stage_ancillary(afire_home, cache_dir, run_dir, input_file, input_dt, afire_options)
            if rc_ancil != 0:
                LOG.warn('Ancillary retrieval failed for input file {}, proceeding...'.format(input_file))
                problem_runs.append(input_file)
                continue

            if not afire_options['ancillary_only']:
                rc,segment_rc,stitched_l1_rc,stitched_l2_rc = \
                        afire_dispatcher(run_dir, afire_home, afire_options, sat_obj)
            else:
                LOG.info("Ancillary ingest only, skipping geocat execution.")
                rc = 0
                segment_rc = [0]
                stitched_l1_rc = 0
                stitched_l2_rc = 0

            if rc != 0 :
                crashed_runs.append(input_file)
            elif np.sum(segment_rc) != 0 :
                problem_runs.append(input_file)
            elif stitched_l1_rc != 0 or stitched_l2_rc != 0 :
                problem_runs.append(input_file)
            else:
                successful_runs.append(input_file)
                if afire_options['docleanup']:
                    cleanup(work_dir, [run_dir])

            LOG.info(">>> Completed processing input file {}\n".format(input_file))


    else:
        LOG.error("Cannot find input input files.")

    '''

    attempted_runs  = list(set(attempted_runs))
    successful_runs = list(set(successful_runs))
    crashed_runs    = list(set(crashed_runs))
    problem_runs    = list(set(problem_runs))

    attempted_runs.sort()
    successful_runs.sort()
    crashed_runs.sort()
    problem_runs.sort()

    return attempted_runs, successful_runs, crashed_runs, problem_runs

def main():
    """
    The main method, which checks envoronment vars and collects all of the required input options.
    Returns 0 on success
    """

    # Read in the command line options
    args, work_dir, docleanup, logfile = argument_parser()

    # Check various paths and environment variables that are "must haves".
    try:

        _, afire_home = check_and_convert_env_var('CSPP_ACTIVE_FIRE_HOME')
        _, afire_ancil_path = check_and_convert_env_var('AFIRE_ANCIL_PATH')
        _ = check_and_convert_path(None, os.path.join(afire_home, 'static_ancillary'), check_write=False)
        _ = check_and_convert_path(None, work_dir, check_write=False)
        ver = check_existing_env_var('NOAA_AFIRE_VER')

    except CsppEnvironment as e:
        LOG.error( e.value )
        LOG.error('Installation error, Make sure all software components were installed.')
        return 2


    afire_options = {}
    afire_options['inputs'] = args.inputs
    afire_options['work_dir'] = os.path.abspath(args.work_dir)
    afire_options['ancil_dir'] = afire_ancil_path
    afire_options['cache_dir'] = setup_cache_dir(args.cache_dir, afire_options['work_dir'],
            'AFIRE_CACHE_PATH')
    afire_options['ancillary_only'] = args.ancillary_only
    afire_options['cache_window'] = args.cache_window
    afire_options['preserve_cache'] = args.preserve_cache
    afire_options['num_cpu'] = args.num_cpu
    afire_options['docleanup'] = docleanup


    rc = 0
    try:

        attempted_runs, successful_runs, crashed_runs, problem_runs = process_afire_inputs(
                afire_home, work_dir, afire_options)

        LOG.info('attempted_runs    {}'.format(attempted_runs))
        LOG.info('successful_runs   {}'.format(successful_runs))
        LOG.info('problem_runs      {}'.format(problem_runs))
        LOG.info('crashed_runs      {}'.format(crashed_runs))

    except Exception:
        LOG.error(traceback.format_exc())
        rc = 1

    return rc


if __name__ == '__main__':
    sys.exit(main())
