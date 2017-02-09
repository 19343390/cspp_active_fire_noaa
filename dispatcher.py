
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
from datetime import datetime

import log_common
from utils import link_files, execute_binary_captured_inject_io

def afire_work_unit(args):
    '''
    This routine encapsulates the single unit of work, multiple instances which are submitted to
    the multiprocessing queue.
    '''

    granule_id = args['granule_id']
    run_dir = args['run_dir']
    cmd = args['cmd']
    env_vars = {}

    rc = 0
    exe_out = "Finished the Active Fires"

    LOG.debug("granule_id = {}".format(granule_id))
    LOG.debug("run_dir = {}".format(run_dir))
    LOG.debug("env_vars = {}".format(env_vars))
    LOG.debug("cmd = {}".format(cmd))

    # Create the run dir for this input file
    log_idx = 0
    while True:
        run_dir = os.path.join(work_dir,"{}_run_{}".format(run_dir,log_idx))
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)
            break
        else:
            log_idx += 1

    LOG.debug("run_dir = {}".format(run_dir))


    # Download and stage the required ancillary data for this input file
    LOG.info("Staging the required ancillary data...")
    #rc_ancil = stage_ancillary(afire_home, cache_dir, run_dir, input_file, input_dt, afire_options)
    #if rc_ancil != 0:
        #LOG.warn('Ancillary retrieval failed for input file {}, proceeding...'.format(input_file))
        #problem_runs.append(input_file)
        #continue

    if not afire_options['ancillary_only']:
        rc,segment_rc,stitched_l1_rc,stitched_l2_rc = \
                create_l2_products(run_dir, afire_home, afire_options, sat_obj)
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

    '''
    # Link the required files and directories into the work directory...
    paths_to_link = [
        os.path.join(afire_home,'l2/{}'.format(sat_obj.geocat_name)),
        os.path.join(afire_home,'l2/data'),
        os.path.join(afire_home,'l2/data_algorithms'),
        os.path.join(afire_home,'l2/geocat.default'),
        geocat_options['tmp_dir'],
        sat_obj.input_dir
    ]

    number_linked = link_files(run_dir, paths_to_link)
    # Contruct a dictionary of error conditions which should be logged.
    error_keys = ['FAILURE', 'failure', 'FAILED', 'failed', 'FAIL', 'fail',
                  'ERROR', 'error', 'ERR', 'err',
                  'ABORTING', 'aborting', 'ABORT','abort']
    error_keys = ['Temporal_Failure'] + error_keys
    error_dict = {x:{'pattern':x, 'count_only':False, 'count':0, 'max_count':None, 'log_str':''}
            for x in error_keys}
    error_dict['error_keys'] = error_keys
    error_dict['Temporal_Failure'] = {
            'pattern':'SYSTEM_BRIDGE_FOG_SERVICES:Have_Fog_Temporal_Data(FAILURE)',
            'count_only':False,
            'count':0,
            'max_count':0,
            'log_str':'{} Temporal failures for segment {}.'}

    rc, exe_out = execute_binary_captured_inject_io(
            run_dir, cmd, error_dict,
            log_execution=False, log_stdout=False, log_stderr=False,
            **env_vars)

    for error_key in ['Temporal_Failure']:
        msg_count = error_dict[error_key]['count']
        if msg_count != 0:
            LOG.warn(error_dict[error_key]['log_str'].format(msg_count, segment))

    LOG.debug(" Granule ID: {}, rc = {}".format(granule_id, rc))
    '''

    return [granule_id, rc, exe_out]


def afire_dispatcher(afire_home, afire_options):
    """
    run active fires to create the level 2 products
    """


    # Run geocat with the specified command line options
    LOG.info("Executing geocat for the area file {} ...".format(sat_obj.input_filename))

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
    for seg_key in sat_obj.segment_data['segment_keys']:
        cmd = sat_obj.cmd[seg_key]
        env_vars = sat_obj.env_vars
        LOG.debug("geocat command...\n\n{}\n".format(cmd))
        afire_tasks.append({'segment':seg_key,'run_dir':run_dir,'cmd':cmd,
            'env_vars':env_vars})


    current_dir = os.getcwd()

    startTime = time.time()

    os.chdir(run_dir)


    # Setup the processing pool
    cpu_count = multiprocessing.cpu_count()
    LOG.info('There are {} available CPUs'.format(cpu_count))
    
    requested_cpu_count = geocat_options['num_cpu']
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

    t_submit = time.time()

    LOG.info("Submitting {} image segments to the pool...".format(len(afire_tasks)))
    result_list = pool.map_async(afire_task_submitter, afire_tasks).get(timeout)

    endTime = time.time()


    total_geocat_time = execution_time(startTime, endTime)
    LOG.debug("geocat execution of {} took {:9.6f} seconds".format(sat_obj.input_filename,total_geocat_time['delta']))
    LOG.info("\tgeocat execution of {} took {} days, {} hours, {} minutes, {:8.6f} seconds"
        .format(sat_obj.input_filename, total_geocat_time['days'],total_geocat_time['hours'],
            total_geocat_time['minutes'],total_geocat_time['seconds']))

    # Open the geocat log file...
    d = datetime.now()
    timestamp = d.isoformat()

    geocat_rc = []
    geocat_hdf_rc = []
    netcdf_rc = []
    segment_rc = []

    # Loop through each of the geocat results and convert the hdf files to netcdf
    for result in result_list:
        seg_key,geocat_seg_rc,exe_out = result
        LOG.debug(">>> Segment {}: geocat_seg_rc = {}".format(seg_key,geocat_seg_rc))

        # Did the actual geocat binary succeed?
        if geocat_seg_rc != 0:
            if geocat_seg_rc == None:
                geocat_seg_rc = 0
            else:
                geocat_seg_rc = 1
        geocat_rc.append(geocat_seg_rc)


        logname = "{}_{}_{}.log".format(run_dir,seg_key,timestamp)
        log_dir = os.path.dirname(run_dir)
        logpath = os.path.join(log_dir, logname)
        logfile_obj = open(logpath,'w')

        # Write the geocat output to a log file, and parse it to determine the output
        # HDF4 files.
        hdf_files = []
        for line in exe_out.splitlines():
            logfile_obj.write(line+"\n")
            searchObj = re.search( r'geocat[LR].*\.hdf', line, re.M)
            if searchObj:
                hdf_files.append(string.split(line," ")[-1])
            else:
                pass

        logfile_obj.close()

        # The run directory for this segment...
        seg_dir = os.path.join(geocat_options['tmp_dir'],sat_obj.segment_data[seg_key]['seg_dir'])

        LOG.debug("hdf_files = {}".format(hdf_files))

        # Check the integrity of the geocat HDF4 files...
        files_to_convert = []
        bad_hdf = False
        for hdf_file in hdf_files:
            LOG.debug("\tseg_dir = {}".format(seg_dir))
            hdf_file = os.path.abspath(os.path.join(seg_dir,os.path.basename(hdf_file)))
            LOG.debug("\thdf_file = {}".format(hdf_file))
            file_size = os.stat(hdf_file).st_size
            LOG.debug("\tSize of HDF4 file {} is {} bytes".format(os.path.basename(hdf_file),
                file_size))
            if file_size < 10000:
                LOG.warn("\tSize of HDF4 file {} is too small, possible geocat problem"
                        .format(os.path.basename(hdf_file)))
                LOG.warn("\tRemoving possibly corrupted HDF4 file {} from the cache."
                        .format(os.path.basename(hdf_file)))
                os.unlink(hdf_file)
                bad_hdf = True
            else:
                files_to_convert.append(hdf_file)

        if bad_hdf:
            geocat_hdf_rc.append(1)
        else:
            geocat_hdf_rc.append(0)

        LOG.debug("HDF4 files to convert: {}".format(files_to_convert))


        # If the HDF4 files are OK, convert to NetCDF4...
        if geocat_hdf_rc[-1] == 0:

            LOG.debug("hdf_files : {}".format(files_to_convert))
            LOG.debug("seg_dir : {}".format(seg_dir))
            
            max_attempts = 3
            num_attempts = 0
            while True:
                num_attempts += 1
                if num_attempts > 3:
                    LOG.error("Maximum of {} conversion attempts reached.".format(max_attempts))
                    break
                else:
                    LOG.debug("Conversion attempt {} ...".format(num_attempts))
                    nc_l2_files,nc_l2_files_rc = hdf4_to_netcdf4(seg_dir, files_to_convert)

                    if nc_l2_files==[] or (1 in nc_l2_files_rc):
                        LOG.warn("Conversion attempt {} failed, trying again...".format(num_attempts))
                        time.sleep(5.)
                    else:
                        break

            LOG.debug("Converted files: {}".format(nc_l2_files))
            
            if nc_l2_files==[] or (1 in nc_l2_files_rc):

                LOG.warn("HDF4 files were not converted to NetCDF4 for : {}\n".format(files_to_convert))
                netcdf_rc.append(1)

            else:

                nc_l2_files_new = sat_obj.nc_filename_from_geocat_filename(nc_l2_files)

                for old_file,new_file in zip(nc_l2_files,nc_l2_files_new):
                    new_filename = os.path.basename(new_file).replace(".nc","_{}.nc".format(seg_key))
                    new_filename = os.path.join(run_dir,new_filename)
                    LOG.debug("Moving {} to {} ({})".format(old_file,new_filename,run_dir))
                    shutil.move(old_file,new_filename)
                LOG.debug("nc_l2_files : {}\n".format(nc_l2_files))
                netcdf_rc.append(0)

        else:
            netcdf_rc.append(0)

    # Boolean "and" the rc arrays, to get a final pass/fail for each segment...
    LOG.debug("geocat_rc:     {}".format(geocat_rc))
    LOG.debug("geocat_hdf_rc: {}".format(geocat_hdf_rc))
    LOG.debug("netcdf_rc:     {}".format(netcdf_rc))

    segment_rc = np.array(geocat_rc,    dtype='bool') + \
                 np.array(geocat_hdf_rc,dtype='bool') + \
                 np.array(netcdf_rc,    dtype='bool')

    segment_rc = np.array(segment_rc,dtype='int')

    os.chdir(current_dir)


    # Check that we have successfully converted all segments to NetCDF4...
    LOG.debug("num_segments: {}".format(num_segments))
    LOG.debug("segment_rc: {}".format(segment_rc))

    # There has been a failure somewhere along the line...
    rc = 0
    if np.sum(segment_rc) != 0:

        if np.sum(geocat_rc) != 0:
            rc = 1
            for segment in range(num_segments):
                if geocat_rc[segment] != 0:
                    LOG.error("Execution of geocat failed to complete for segment {}".format(segment))

        if np.sum(geocat_hdf_rc) != 0:
            for segment in range(num_segments):
                if geocat_hdf_rc[segment] != 0:
                    LOG.error("geocat output HDF4 file may be corrupted for segment {}".format(segment))

        if np.sum(netcdf_rc) != 0:
            for segment in range(num_segments):
                if netcdf_rc[segment] != 0:
                    LOG.error("HDF4 to NetCDF4 ouput file conversion failed for segment {}".format(segment))

        stitched_l1_rc = 0
        stitched_l2_rc = 0

    else:

        # Stitch the files together...
        geocat_l1_files = glob(os.path.join(run_dir,"geocatL1*.nc"))
        geocat_l2_files = glob(os.path.join(run_dir,"geocatL2*.nc"))
        geocat_l1_files.sort()
        geocat_l2_files.sort()
        LOG.debug("geocat_l1_files: {}".format(geocat_l1_files))
        LOG.debug("geocat_l2_files: {}".format(geocat_l2_files))

        stitched_dir = geocat_options['work_dir']
        segments = [geocat_options['seg_rows'], geocat_options['seg_cols']]

        if geocat_l1_files != [] :
            filename = os.path.basename(geocat_l1_files[0])
            stitched_filename = os.path.join(stitched_dir,"{}.nc".format(filename.split("_")[0]))
            LOG.debug("Stitched level 1 file = {}".format(stitched_filename))
            stitched_l1_rc = stitch_files(geocat_l1_files,segments,stitched_filename)
        else:
            stitched_l1_rc = 1

        if geocat_l2_files != [] :
            filename = os.path.basename(geocat_l2_files[0])
            stitched_filename = os.path.join(stitched_dir,"{}.nc".format(filename.split("_")[0]))
            LOG.debug("Stitched level 2 file = {}".format(stitched_filename))
            stitched_l2_rc = stitch_files(geocat_l2_files,segments,stitched_filename)
        else:
            stitched_l2_rc = 1



    return rc,segment_rc,stitched_l1_rc,stitched_l2_rc
