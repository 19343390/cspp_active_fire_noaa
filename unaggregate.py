#!/usr/bin/env python
# encoding: utf-8
"""
unaggregate.py


 * DESCRIPTION: This file contains routines for finding and unaggregating VIIRS level-1 files.

Created by Geoff Cureton on 2017-04-24.
Copyright (c) 2017 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
import logging
import time
import multiprocessing
import traceback
from datetime import datetime

from utils import create_dir, link_files, execution_time, execute_binary_captured_inject_io

LOG = logging.getLogger('unaggregate')


def find_aggregated(data_dict):
    '''
    Find aggregated input files in the input data dict, remove them from the dict and return them
    in a list.
    '''

    aggregated_list = []

    for granule_id in data_dict.keys():
        for kind in data_dict[granule_id].keys():
            try:
                if data_dict[granule_id][kind]['is_aggregated']:
                    aggregated_list.append(data_dict[granule_id][kind]['file'])
                    LOG.debug("Removing entry '{}' from input dictionary granule ID {}".format(
                        kind, granule_id))
                    del(data_dict[granule_id][kind])
                else:
                    pass
            except:
                LOG.warn("Unable to remove entry '{}' from input dictionary granule ID {}".format(
                    kind, granule_id))
                LOG.debug(traceback.format_exc())

            try:
                if data_dict[granule_id] == {}:
                    LOG.debug("Removing empty dictionary for granule ID '{}'...".format(granule_id))
                    del(data_dict[granule_id])
                else:
                    pass
            except:
                LOG.warn("Unable to remove empty dictionary for granule ID '{}'".format(granule_id))
                LOG.debug(traceback.format_exc())

    aggregated_list.sort()

    return aggregated_list


def nagg_submitter(args):
    '''
    This routine encapsulates the single unit of work, multiple instances of which are submitted to
    the multiprocessing queue. It takes as input whatever is required to complete the work unit,
    and returns return values and output logging from the external process.
    '''

    # This try block wraps all code in this worker function, to capture any exceptions.
    try:

        afire_home = args['afire_home']
        agg_input_file = args['agg_input_file']
        unagg_inputs_dir = args['unagg_inputs_dir']
        afire_options = args['afire_options']
        work_dir = afire_options['work_dir']
        env_vars = {}

        rc_exe = 0
        rc_problem = 0
        exe_out = "Finished running nagg on the aggregated VIIRS file: {}".format(agg_input_file)

        LOG.debug("afire_home = {}".format(afire_home))
        LOG.debug("agg_input_file = {}".format(agg_input_file))
        LOG.debug("unagg_inputs_dir = {}".format(unagg_inputs_dir))
        LOG.debug("work_dir = {}".format(work_dir))
        LOG.debug("env_vars = {}".format(env_vars))

        current_dir = os.getcwd()

        LOG.info("Processing aggregated file {}...".format(agg_input_file))

        os.chdir(unagg_inputs_dir)

        nagg_exe = os.path.join(afire_home, 'vendor/nagg')
        nagg_exe = './nagg'
        prefix = os.path.basename(agg_input_file).split('_')[0]

        cmd_dict = {}
        cmd_dict['GEO'] = '{} -S -g {} -n 1 -O cspp -D dev -d {} {}'.format(
            nagg_exe, prefix, unagg_inputs_dir, agg_input_file)
        cmd_dict['SVM'] = '{} -t {} -S -g no -n 1 -O cspp -D dev -d {} {}'.format(
            nagg_exe, prefix, unagg_inputs_dir, agg_input_file)

        if 'GMTCO' in os.path.basename(agg_input_file):
            cmd = cmd_dict['GEO']
        elif 'GITCO' in os.path.basename(agg_input_file):
            cmd = cmd_dict['GEO']
        elif 'SVM' in os.path.basename(agg_input_file):
            cmd = cmd_dict['SVM']
        elif 'SVI' in os.path.basename(agg_input_file):
            cmd = cmd_dict['SVM']
        elif 'IVCDB' in os.path.basename(agg_input_file):
            cmd = cmd_dict['SVM']
        else:
            cmd = None
        #cmd = '''sleep 0.5; echo "Running nagg on {0:}"; exit 0'''.format(
                #os.path.basename(agg_input_file))

        # Contruct a dictionary of error conditions which should be logged.
        error_keys = ['FAILURE', 'failure', 'FAILED', 'failed', 'FAIL', 'fail',
                      'ERROR', 'error', 'ERR', 'err',
                      'ABORTING', 'aborting', 'ABORT', 'abort']
        error_dict = {x: {'pattern': x, 'count_only': False, 'count': 0, 'max_count': None,
                          'log_str': ''}
                      for x in error_keys}
        error_dict['error_keys'] = error_keys

        if cmd is not None:
            start_time = time.time()

            rc_exe, exe_out = execute_binary_captured_inject_io(
                unagg_inputs_dir, cmd, error_dict,
                log_execution=False, log_stdout=False, log_stderr=False,
                **env_vars)

            end_time = time.time()

            nagg_time = execution_time(start_time, end_time)
            LOG.debug("\tnagg execution of {} took {:9.6f} seconds".format(
                os.path.basename(agg_input_file), nagg_time['delta']))
            LOG.info(
                "\tnagg execution for {} took {} days, {} hours, {} minutes, {:8.6f} seconds"
                .format(os.path.basename(agg_input_file), nagg_time['days'], nagg_time['hours'],
                        nagg_time['minutes'], nagg_time['seconds']))

            LOG.debug("\tnagg({}), rc_exe = {}".format(os.path.basename(agg_input_file), rc_exe))
        else:
            exe_out = '''Aggregated file {} cannot be unaggregated by nagg,''' \
                ''' unrecognized prefix {}.'''.format(prefix, os.path.basename(agg_input_file))
            LOG.warn('\t' + exe_out)

        # Write the afire output to a log file, and parse it to determine the output
        creation_dt = datetime.utcnow()
        timestamp = creation_dt.isoformat()
        logname = "nagg_unaggregate-{}-{}.log".format(os.path.basename(agg_input_file), timestamp)
        logpath = os.path.join(unagg_inputs_dir, logname)
        logfile_obj = open(logpath, 'w')
        for line in exe_out.splitlines():
            logfile_obj.write(line + "\n")
        logfile_obj.close()

        os.chdir(current_dir)

    except Exception:
        LOG.warn("\tGeneral warning for {}".format(os.path.basename(agg_input_file)))
        LOG.debug(traceback.format_exc())
        os.chdir(current_dir)
        raise

    return [os.path.basename(agg_input_file), rc_exe, rc_problem, exe_out]


def unaggregate_inputs(afire_home, agg_input_files, afire_options):
    '''
    Create a dir for the unaggregated files in the work dir, and use nagg to unaggregate the
    aggregated input files.
    '''

    unagg_inputs_dir = os.path.join(afire_options['work_dir'], 'unaggregated_inputs')
    unagg_inputs_dir = create_dir(unagg_inputs_dir)

    # Construct a list of task dicts...
    nagg_tasks = []
    for agg_input_file in agg_input_files:
        args = {'afire_home': afire_home,
                'agg_input_file': agg_input_file,
                'unagg_inputs_dir': unagg_inputs_dir,
                'afire_options': afire_options}
        nagg_tasks.append(args)

    # Link the nagg executable into the unaggregated inputs dir...
    paths_to_link = [os.path.join(afire_home, 'vendor/nagg')]
    number_linked = link_files(unagg_inputs_dir, paths_to_link)
    LOG.debug("\tWe are linking {} files to the run dir:".format(number_linked))
    for linked_files in paths_to_link:
        LOG.debug("\t{}".format(linked_files))

    # Setup the processing pool
    cpu_count = multiprocessing.cpu_count()
    LOG.debug('There are {} available CPUs'.format(cpu_count))

    requested_cpu_count = afire_options['num_cpu']

    if requested_cpu_count is not None:
        LOG.debug('We have requested {} {}'.format(
            requested_cpu_count, "CPU" if requested_cpu_count == 1 else "CPUs"))

        if requested_cpu_count > cpu_count:
            LOG.warn('{} requested CPUs is greater than available, using {}'.format(
                requested_cpu_count, cpu_count))
            cpus_to_use = cpu_count
        else:
            cpus_to_use = requested_cpu_count
    else:
        cpus_to_use = cpu_count

    LOG.debug('We are using {}/{} available CPUs'.format(cpus_to_use, cpu_count))
    pool = multiprocessing.Pool(cpus_to_use)

    # Submit the Active Fire tasks to the processing pool
    timeout = 9999999
    result_list = []

    start_time = time.time()

    LOG.info("Submitting {} nagg {} to the pool...".format(
        len(nagg_tasks), "task" if len(nagg_tasks) == 1 else "tasks"))
    result_list = pool.map_async(nagg_submitter, nagg_tasks).get(timeout)

    end_time = time.time()

    total_afire_time = execution_time(start_time, end_time)
    LOG.debug("Unaggregation took {:9.6f} seconds".format(total_afire_time['delta']))
    LOG.info(
        "Unaggregation took {} days, {} hours, {} minutes, {:8.6f} seconds"
        .format(total_afire_time['days'], total_afire_time['hours'],
                total_afire_time['minutes'], total_afire_time['seconds']))

    # Loop through each of the Active Fire results collect error information
    for result in result_list:
        agg_input_file, nagg_rc, problem_rc, exe_out = result
        LOG.debug(">>> agg_input_file {}: nagg_rc = {}, problem_rc = {}".format(
            agg_input_file, nagg_rc, problem_rc))

    return unagg_inputs_dir
