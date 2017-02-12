#!/usr/bin/env python
# encoding: utf-8
"""
dispatcher.py

 * DESCRIPTION: This file contains methods to construct a list of valid command line invocations,
 is the collection of utilities for parsing text, running external processes and
 binaries, checking environments and whatever other mundane tasks aren't specific to this project.

Created by Geoff Cureton on 2017-02-07.
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
import traceback
from subprocess import Popen, CalledProcessError, call, PIPE
import multiprocessing
from datetime import datetime

import log_common
from utils import link_files, execution_time, execute_binary_captured_inject_io, cleanup

#import ancillary
from ancillary import get_lwm
#from ancillary import GridIP

LOG = logging.getLogger('dispatcher')

def afire_submitter(args):
    '''
    This routine encapsulates the single unit of work, multiple instances which are submitted to
    the multiprocessing queue. It takes as input whatever is required to complete the work unit,
    and returns return values and output logging from the external process.
    '''

    granule_dict = args['granule_dict']
    afire_home = args['afire_home']
    afire_options = args['afire_options']

    granule_id = granule_dict['granule_id']
    run_dir = granule_dict['run_dir']
    cmd = granule_dict['cmd']
    work_dir = afire_options['work_dir']
    env_vars = {}

    rc = 0
    exe_out = "Finished the Active Fires"

    LOG.debug("granule_id = {}".format(granule_id))
    LOG.debug("run_dir = {}".format(run_dir))
    LOG.debug("cmd = {}".format(cmd))
    LOG.debug("work_dir = {}".format(work_dir))
    LOG.debug("env_vars = {}".format(env_vars))

    current_dir = os.getcwd()
    LOG.debug("We are in {}".format(os.getcwd()))

    # Create the run dir for this input file
    log_idx = 0
    LOG.debug("Creating a run dir...")
    while True:
        run_dir = os.path.join(work_dir,"{}_run_{}".format(granule_dict['run_dir'],log_idx))
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)
            break
        else:
            log_idx += 1

    LOG.debug("run_dir = {}".format(run_dir))

    os.chdir(run_dir)
    LOG.debug("We are in {}".format(os.getcwd()))

    # Download and stage the required ancillary data for this input file
    LOG.info("Staging the required ancillary data...")
    rc_ancil, anc_dir = get_lwm(afire_options, granule_dict)
    if rc_ancil != 0:
        LOG.warn('Ancillary retrieval failed for granule_id {}, proceeding...'.format(granule_id))
        problem_runs.append(input_file)
        return 1
    LOG.info("Persistent anc_dir['{}'] = {}".format(granule_id, anc_dir))
    lwm_file = os.path.join(afire_options['cache_dir'], anc_dir, granule_dict['GRLWM']['file'])
    LOG.info("lwm file to link = {}".format(lwm_file))
    #for key in granule_dict.keys():
        #LOG.info("{} : {} = {}".format(granule_id, key, granule_dict[key]))

    # Link the required files and directories into the work directory...
    paths_to_link = [
        os.path.join(afire_home,'vendor/vfire'),
        lwm_file,
    ] + [granule_dict[key]['file'] for key in ['GMTCO','SVM05', 'SVM07', 'SVM11', 'SVM13', 'SVM15', 'SVM16']]
    number_linked = link_files(run_dir, paths_to_link)

    # Contruct a dictionary of error conditions which should be logged.
    error_keys = ['FAILURE', 'failure', 'FAILED', 'failed', 'FAIL', 'fail',
                  'ERROR', 'error', 'ERR', 'err',
                  'ABORTING', 'aborting', 'ABORT','abort']
    error_dict = {x:{'pattern':x, 'count_only':False, 'count':0, 'max_count':None, 'log_str':''}
            for x in error_keys}
    error_dict['error_keys'] = error_keys

    start_time = time.time()

    rc, exe_out = execute_binary_captured_inject_io(
            run_dir, cmd, error_dict,
            log_execution=False, log_stdout=False, log_stderr=False,
            **env_vars)

    end_time = time.time()

    afire_time = execution_time(start_time, end_time)
    LOG.debug("afire execution of {} took {:9.6f} seconds".format(granule_id, afire_time['delta']))
    LOG.info("\tafire execution of {} took {} days, {} hours, {} minutes, {:8.6f} seconds"
        .format(granule_id, afire_time['days'],afire_time['hours'],
            afire_time['minutes'],afire_time['seconds']))

    LOG.debug(" Granule ID: {}, rc = {}".format(granule_id, rc))

    os.chdir(current_dir)
    LOG.debug("We are in {}".format(os.getcwd()))

    # Write the afire output to a log file, and parse it to determine the output
    d = datetime.now()
    timestamp = d.isoformat()
    logname = "{}_{}.log".format(run_dir, timestamp)
    log_dir = os.path.dirname(run_dir)
    logpath = os.path.join(log_dir, logname)
    logfile_obj = open(logpath,'w')
    for line in exe_out.splitlines():
        logfile_obj.write(line+"\n")
    logfile_obj.close()

    # If no problems, remove the run dir
    if (rc == 0) and afire_options['docleanup']:
            cleanup(work_dir, [run_dir])

    return [granule_id, rc, exe_out]


def afire_dispatcher(afire_home, afire_data_dict, afire_options):
    """
    Run active fires to create the level 2 products
    """

    granule_id_list = afire_data_dict.keys()
    granule_id_list.sort()

    '''
    To deliberately throw a segfault for testing, we can set...

        cmd = 'echo "This is a test cmd to throw a segfault..." ; kill -11 $$'

    or compile a custom C exe...

        echo "int main() { *((char *)0) = 0; }" > segfault_get.c
        gcc segfault_get.c -o segfault_get

    and then set...

        cmd = '/mnt/WORK/work_dir/test_data/sample_data/segfault_get'

    which should generate a return code of -11 (segfault).
    '''

    # A couple of commands which fail to produce output...
    #sat_obj.cmd['seg_2'] = 'sleep 0.5'
    #sat_obj.cmd['seg_2'] = 'sleep 0.5; exit 1'
    #sat_obj.cmd = {x:'sleep 0.5; exit 1' for x in sat_obj.segment_data['segment_keys']}
    #sat_obj.cmd['seg_2'] = '/mnt/WORK/work_dir/segfault_test/segfault_get'
    #sat_obj.cmd = {x:'sleep 0.5; echo "geocat>> Cannot create HDF writing for SDS, cloud_spherical_albedo - aborting."' for x in sat_obj.segment_data['segment_keys']}

    # Construct a list of task dicts...
    afire_tasks = []
    for granule_id in granule_id_list:
        args = {'granule_dict':afire_data_dict[granule_id],
                'afire_home':afire_home,
                'afire_options':afire_options}
        afire_tasks.append(args)

    # Setup the processing pool
    cpu_count = multiprocessing.cpu_count()
    LOG.info('There are {} available CPUs'.format(cpu_count))
    
    requested_cpu_count = afire_options['num_cpu']
    LOG.info('We have requested {} CPUs'.format(requested_cpu_count))
    
    if requested_cpu_count > cpu_count:
        LOG.warn('{} requested CPUs is created than available, using {}'.format(
            requested_cpu_count,cpu_count))
        cpus_to_use = cpu_count
    else:
        cpus_to_use = requested_cpu_count
    
    LOG.info('We are using {}/{} available CPUs'.format(cpus_to_use,cpu_count))

    pool = multiprocessing.Pool(cpus_to_use)

    timeout = 9999999
    result_list = []

    start_time = time.time()

    LOG.info("Submitting {} image segments to the pool...".format(len(afire_tasks)))
    result_list = pool.map_async(afire_submitter, afire_tasks).get(timeout)

    end_time = time.time()


    total_afire_time = execution_time(start_time, end_time)
    LOG.debug("afire execution took {:9.6f} seconds".format(total_afire_time['delta']))
    LOG.info("\tafire execution took {} days, {} hours, {} minutes, {:8.6f} seconds"
        .format(total_afire_time['days'],total_afire_time['hours'],
            total_afire_time['minutes'],total_afire_time['seconds']))

    # Open the afire log file...
    d = datetime.now()
    timestamp = d.isoformat()

    rc_dict = {}

    # Loop through each of the afire results and convert the hdf files to netcdf
    for result in result_list:
        granule_id, afire_rc, exe_out = result
        LOG.debug(">>> granule_id {}: afire_rc = {}".format(granule_id, afire_rc))

        # Did the actual afire binary succeed?
        rc_dict[granule_id] = afire_rc

    # Boolean "and" the rc arrays, to get a final pass/fail for each segment...
    LOG.debug("rc_dict:     {}".format(rc_dict))

    return rc_dict
