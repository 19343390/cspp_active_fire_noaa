#!/usr/bin/env python
# encoding: utf-8
"""
$Id:$

Purpose:


Copyright (c) 2011 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
import sys
import re, string
import shutil
import logging
import time
from glob import glob
import numpy as np
import traceback
from datetime import datetime
import multiprocessing


from basics import link_files,execute_binary_captured_inject_io
#import log_common
from convert import hdf4_2_netcdf4 as H4toNC4
import satellites
from multiproc import stitch_files

LOG = logging.getLogger('geocat_interface')


def execution_time(startTime, endTime):
    '''
    Converts a time duration in seconds to days, hours, minutes etc...
    '''

    time_dict = {}

    delta = endTime - startTime
    days, remainder = divmod(delta, 86400.)
    hours, remainder = divmod(remainder, 3600.)
    minutes, seconds = divmod(remainder, 60.)

    time_dict['delta']   = delta
    time_dict['days']    = int(days)
    time_dict['hours']   = int(hours)
    time_dict['minutes'] = int(minutes)
    time_dict['seconds'] = seconds

    return time_dict


def afire_task_submitter(args):

    segment = args['segment']
    run_dir = args['run_dir']
    cmd = args['cmd']
    env_vars = args['env_vars']

    env_vars['segment'] = segment

    LOG.debug("segment = {}".format(segment))
    LOG.debug("run_dir = {}".format(run_dir))
    LOG.debug("env_vars = {}".format(env_vars))
    LOG.debug("cmd = {}".format(cmd))

    rc,exe_out = execute_binary_captured_inject_io(
            run_dir, cmd,
            log_execution=False, log_stdout=False, log_stderr=False,
            **env_vars)

    LOG.debug("Segment {} of {}: rc = {}".format(segment,cmd.split(" ")[-1],rc))


    return [segment,rc,exe_out]


def hdf4_to_netcdf4(work_dir, hdf_files):
    """
    Takes a list of HDF4 and converts them to NetCDF4.
    """

    ret_val = 0
    nc_files = []
    nc_files_rc = []

    if hdf_files:

        # Check that the HDF4 files exist...
        for hdf_file in hdf_files:
            if not os.path.exists(hdf_file):
                LOG.warning("File does not exist: {}".format(hdf_file))

        LOG.debug("Converting the HDF4 output files to NetCDF4...")
        LOG.debug("hdf_files : {}".format(hdf_files))
        convert_rc = H4toNC4(work_dir,hdf_files)
        LOG.debug("convert_rc : {}".format(convert_rc))

        for hdf_file,rc in zip(hdf_files,convert_rc):
            # Construct the netcdf4 file name...
            hdf_file_base = string.split(os.path.basename(hdf_file),".hdf")[0]
            nc_filename = os.path.join(work_dir,"{}.nc".format(hdf_file_base))
            nc_file = glob(nc_filename)

            LOG.debug("Checking conversion of {} to {} ...".format(hdf_file,nc_filename))

            # Check that the converter reported succcess, and the netcdf4 files exists...
            if nc_file and (rc==0):
                LOG.debug("\tSuccessfully converted {} to {}.".format(
                    os.path.basename(hdf_file),
                    os.path.basename(nc_file[0])))
                nc_files.append(nc_file[0])
                nc_files_rc.append(0)
            else:
                LOG.warning("\tProblem converting {} to {}".format(
                    os.path.basename(hdf_file),
                    os.path.basename(nc_filename)))
                nc_files.append(None)
                nc_files_rc.append(1)

    return nc_files,nc_files_rc


def create_l2_products(run_dir,geo_home,geocat_options,sat_obj):
    """
    run geocat to create the level 2 products
    """

    # Link the required files and directories into the work directory...
    paths_to_link = [
        os.path.join(geo_home,'l2/{}'.format(sat_obj.geocat_name)),
        os.path.join(geo_home,'l2/data'),
        os.path.join(geo_home,'l2/data_algorithms'),
        os.path.join(geo_home,'l2/geocat.default'),
        geocat_options['tmp_dir'],
        sat_obj.input_dir
    ]
    number_linked = link_files(run_dir, paths_to_link)

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

    # Scrape some info from the input file...
    sat_obj.get_input_file_metadata()

    # Setting satellite viewport
    sat_obj.get_image_limits()

    # Generate the segmentation information for this image
    sat_obj.generate_segments()

    for seg_key in sat_obj.segment_data['segment_keys']:
        ystart  = sat_obj.segment_data[seg_key]['ystart']
        yend    = sat_obj.segment_data[seg_key]['yend']
        xstart  = sat_obj.segment_data[seg_key]['xstart']
        xend    = sat_obj.segment_data[seg_key]['xend']
        xstride = sat_obj.segment_data[seg_key]['xstride']
        seg_dir = sat_obj.segment_data[seg_key]['seg_dir']
        LOG.debug("{:6s} -> ystart : {:5d}, yend : {:5d}, xstart : {:5d}, xend : {:5d}, xstride : {:3d}, seg_dir : {}"
                .format(seg_key,ystart, yend, xstart, xend, xstride, seg_dir))

    num_segments = len(sat_obj.segment_data['segment_keys'])

    # Set the various geocat options.
    sat_obj.set_satellite_options()

    # Make the required segment directories...
    for seg_key in sat_obj.segment_data['segment_keys']:
        seg_dir = os.path.join(geocat_options['tmp_dir'],sat_obj.segment_data[seg_key]['seg_dir'])
        if os.path.isdir(seg_dir) :
            LOG.debug("Removing existing segment directory: {}".format(seg_dir))
            shutil.rmtree(seg_dir)
        LOG.debug("Creating the segment directory: {}".format(seg_dir))
        os.makedirs(seg_dir)

    # A couple of commands which fail to produce output...
    #sat_obj.cmd['seg_2'] = 'sleep 0.5'
    #sat_obj.cmd['seg_2'] = 'sleep 0.5; exit 1'
    #sat_obj.cmd = {x:'sleep 0.5; exit 1' for x in sat_obj.segment_data['segment_keys']}
    #sat_obj.cmd['seg_2'] = '/mnt/WORK/work_dir/segfault_test/segfault_get'
    #sat_obj.cmd = {x:'sleep 0.5; echo "geocat>> Cannot create HDF writing for SDS, cloud_spherical_albedo - aborting."' for x in sat_obj.segment_data['segment_keys']}

    # Construct a list of task dicts...
    geocat_tasks = []
    for seg_key in sat_obj.segment_data['segment_keys']:
        cmd = sat_obj.cmd[seg_key]
        env_vars = sat_obj.env_vars
        LOG.debug("geocat command...\n\n{}\n".format(cmd))
        geocat_tasks.append({'segment':seg_key,'run_dir':run_dir,'cmd':cmd,
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

    LOG.info("Submitting {} image segments to the pool...".format(len(geocat_tasks)))
    result_list = pool.map_async(afire_task_submitter, geocat_tasks).get(timeout)

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
