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
from dispatcher import afire_dispatcher
from active_fire_interface import generate_file_list, construct_cmd_invocations
from utils import  setup_cache_dir, clean_cache, link_files, CsppEnvironment
from utils import check_and_convert_path, check_and_convert_env_var, check_existing_env_var

os.environ['TZ'] = 'UTC'

LOG = logging.getLogger(__name__)

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
        #LOG.info('anc_dir[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['anc_dir']))
        LOG.info('anc_file[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['GRLWM']['file']))
        LOG.info('run_dir[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['run_dir']))
        LOG.info('cmd[{}] = {}'.format(granule_id, afire_data_dict[granule_id]['cmd']))

    # Clean out product cache files that are too old.
    LOG.info('')
    if not afire_options['preserve_cache']:
        LOG.info(">>> Cleaning the ancillary cache back {} hours...".format(afire_options['cache_window']))
        first_dt = afire_data_dict[granule_id_list[0]]['GMTCO']['dt']
        clean_cache(afire_options['cache_dir'], afire_options['cache_window'], first_dt)

    # Run the dispatcher
    rc_dict = afire_dispatcher(afire_home, afire_data_dict, afire_options)
    LOG.info("rc_dict = {}".format(rc_dict))

    for granule_id in granule_id_list:
        attempted_runs.append(granule_id)
        if rc_dict[granule_id] == 0:
            successful_runs.append(granule_id)
        else:
            crashed_runs.append(granule_id)

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
