#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import re, string
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
#from active_fire_interface import execution_time, create_l2_products

from basics import link_files, check_and_convert_path, check_and_convert_env_var, \
        check_existing_env_var, CsppEnvironment

os.environ['TZ'] = 'UTC'

LOG = logging.getLogger(__name__)

def _convert_datetime(s):
    "converter which takes strings from ASC and converts to computable datetime objects"
    pt = s.rfind('.')
    micro_s = s[pt+1:]
    micro_s += '0'*(6-len(micro_s))
    #when = dt.datetime.strptime(s[:pt], '%Y-%m-%d %H:%M:%S').replace(microsecond = int(micro_s))
    when = datetime.strptime(s[:pt], '%Y-%m-%d %H:%M:%S').replace(microsecond = int(micro_s))
    return when

def _convert_isodatetime(s):
    "converter which takes strings from ASC and converts to computable datetime objects"
    pt = s.rfind('.')
    micro_s = s[pt+1:]
    micro_s += '0'*(6-len(micro_s))
    #when = dt.datetime.strptime(s[:pt], '%Y-%m-%d %H:%M:%S').replace(microsecond = int(micro_s))
    when = datetime.strptime(s[:pt], '%Y-%m-%dT%H:%M:%S').replace(microsecond = int(micro_s))
    return when

def get_granule_ID(IET_StartTime):
    """
    Calculates the deterministic granule ID. From...
    ADL/CMN/Utilities/INF/util/gran/src/InfUtil_GranuleID.cpp
    """
    # NPP_GRANULE_ID_BASETIME corresponds to the number of microseconds between 
    # datetime(2011, 10, 23, 0, 0, 0) and the IDPS epoch time (IET) datetime(1958,1,1), plus the 34
    # leap seconds that had been added between the IET epoch and 2011.
    NPP_GRANULE_ID_BASETIME = int(os.environ.get('NPP_GRANULE_ID_BASETIME', 1698019234000000))
    granuleSize = 85350000      # microseconds

    # Subtract the spacecraft base time from the arbitrary time to obtain
    # an elapsed time. 
    elapsedTime = IET_StartTime - NPP_GRANULE_ID_BASETIME

    # Divide the elapsed time by the granule size to obtain the granule number; 
    # the integer division will give the desired floor value.
    granuleNumber = int(np.floor(elapsedTime / granuleSize))
    #granuleNumber = np.ceil(elapsedTime / granuleSize)

    # Multiply the granule number by the granule size, then add the spacecraft
    # base time to obtain the granule start boundary time. Add the granule
    # size to the granule start boundary time to obtain the granule end
    # boundary time.
    #startBoundary = (granuleNumber * granuleSize) + NPP_GRANULE_ID_BASETIME
    #endBoundary = startBoundary + granuleSize

    # assign the granule start and end boundary times to the class members 
    #granuleStartTime = startBoundary
    #granuleEndTime = endBoundary
    
    # multiply the granule number by the granule size
    # then divide by 10^5 to convert the microseconds to tenths of a second; 
    # the integer division will give the desired floor value.
    timeCode = int(float(granuleNumber * granuleSize) / 100000.)

    N_Granule_ID = 'NPP{0:0>12d}'.format(timeCode)

    return N_Granule_ID

def get_granule_id_from_filename(filename,pattern,epoch,leapsec_dt_list):
    '''
    Computes a datetime object from "filename" using the regex "pattern", and determines the 
    elapsed time since "epoch".
    '''
    # Compile the regular expression for the filename...
    re_pattern = re.compile(pattern)

    # Get some information based on the filename
    file_basename = os.path.basename(filename)
    LOG.debug("file_basename = {}".format(file_basename))
    file_info = dict(re_pattern.match(file_basename).groupdict())
    LOG.debug("file_info = {}".format(file_info))

    # Determine the granule ID.
    dt_string = "{}_{}".format(file_info['date'],file_info['start_time'])
    LOG.debug("dt_string = {}".format(dt_string))
    dt = datetime.strptime(dt_string,"%Y%m%d_%H%M%S%f")
    LOG.debug("dt = {}".format(dt))
    leap_seconds = int(get_leapseconds(leapsec_dt_list,dt))
    LOG.debug("leap_seconds = {}".format(leap_seconds))
    iet_time = int(((dt - epoch).total_seconds() + leap_seconds) * 1000000.)
    LOG.debug("iet_time = {}".format(iet_time))
    granule_id = get_granule_ID(iet_time)

    return granule_id, file_info, dt

def get_leapsec_table():
    '''
    Read the IETTime.dat file containing the leap seconds since 1972, and save into a list of dicts.
    '''

    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    month_enum = {item:idx for idx,item in enumerate(months, start=1)}
    afire_ancil_path = check_and_convert_env_var('AFIRE_ANCIL_PATH')
    leapsec_filename = os.path.join(afire_ancil_path,'IETTime.dat')
    try:
        leapsec_file = open(leapsec_filename,"ro") # Open template file for reading
    except Exception, err :
        LOG.error("{}, aborting.".format(err))

    leapsec_dt_list = []
    for line in leapsec_file.readlines():
        line = line.replace("\n","")
        fields = line.split(" ")
        fields = filter(lambda x: x != '', fields)
        year = int(fields[0])
        month = month_enum[fields[1]]
        day = int(fields[2])
        leap_secs = int(float(fields[6]))
        leap_dt = datetime(year,month,day)
        leapsec_dt_list.append({'dt':leap_dt, 'leapsecs':leap_secs})
    leapsec_file.close()

    return leapsec_dt_list

def get_leapseconds(leapsec_table,dt):
    '''
    Compares a datetime object to those in a table, and returns the correct number
    of leapseconds to add to the epoch time.
    '''
    leap_seconds = 0
    delta = 0

    for leapsec_dict in leapsec_table:
        temp_leap_seconds = leapsec_dict['leapsecs']
        delta = (dt - leapsec_dict['dt']).total_seconds()
        if delta < 0:
            return leap_seconds
        leap_seconds = temp_leap_seconds

    return leap_seconds

def generate_file_list(inputs,full=False):
    '''
    Trawl through the files and directories given at the command line, pick out those matching the 
    desired file types, and att them to a master list of raw data files. This list need not be 
    sorted into time order.
    '''

    input_files = []
    data_list = []

    for input in inputs:
        LOG.debug("bash glob input = {}".format(input))

    if full:
        input_files = list(set(inputs))
        input_files.sort()
        return input_files

    input_prefixes = ['GMTCO','SVM05','SVM07', 'SVM11', 'SVM13', 'SVM15', 'SVM16']

    input_dirs = []
    input_files = []

    # Sort the command line inputs into directory and file inputs...
    for input in inputs:
        input = os.path.abspath(os.path.expanduser(input))
        if os.path.isdir(input) :
            # Input file glob is of form "/path/to/files"
            LOG.debug("Input {} is a directory containing files...".format(input))
            input_dirs.append(input)
        elif os.path.isfile(input) :
            ## Input file glob is of form "/path/to/files/goes13_1_2015_143_1745.input"
            LOG.debug("Input {} is a file.".format(input))
            input_files.append(input)

    input_dirs = list(set(input_dirs))
    input_dirs.sort()
    input_files = list(set(input_files))
    input_files.sort()

    for dirs in input_dirs:
        LOG.info("input dirs {}".format(dirs))
    for files in input_files:
        LOG.info("input files {}".format(files))


    # The re defining the fields of an NPP CDFCB-format filename
    RE_NPP_list = ['(?P<kind>[A-Z]+)(?P<band>[0-9]*)_',
            '(?P<sat>[A-Za-z0-9]+)_','d(?P<date>\d+)_',
            't(?P<start_time>\d+)_',
            'e(?P<end_time>\d+)_b(?P<orbit>\d+)_',
            'c(?P<created_time>\d+)_',
            '(?P<site>[a-zA-Z0-9]+)_',
            '(?P<domain>[a-zA-Z0-9]+)\.h5']
    RE_NPP_str = "".join(RE_NPP_list)
    
    # Get a table of the leap seconds
    iet_epoch = datetime(1958,1,1)
    LOG.debug("Epoch time: {}".format(iet_epoch))
    leapsec_dt_list = get_leapsec_table()

    # Loop through the input dirs and record any desired files in any of these directories

    data_list = {}
    for dirs in input_dirs:
        for input_prefix in input_prefixes:
            input_glob = '{}*.h5'.format(input_prefix)
            input_glob = os.path.join(dirs, input_glob)
            LOG.debug("input glob is {}".format(input_glob))
            temp_input_files = glob.glob(input_glob)
            temp_input_files.sort()
            LOG.debug("temp_input_files {}".format(temp_input_files))

            for files in temp_input_files:

                granule_id, file_info, dt = get_granule_id_from_filename(files, RE_NPP_str, iet_epoch, 
                        leapsec_dt_list)

                LOG.debug("granule_id = {}".format(granule_id))

                kind_key = '{}{}'.format(file_info['kind'],file_info['band'])
                try:
                    data_list[granule_id][kind_key] = file_info
                except KeyError:
                    LOG.debug("Entry for granule ID {} does not yet exist, creating...".format(granule_id))
                    data_list[granule_id] = {}
                    data_list[granule_id][kind_key] = file_info

                data_list[granule_id][kind_key]['file'] = files
                data_list[granule_id][kind_key]['dt'] = dt

    # Loop through the input files, determine their dirs, and sweep up of the files in those dirs
    # that share granule ids with the command line inputs.

    # Get the granule ids of the files given in the input
    granule_id_from_files = []

    for files in input_files:
        LOG.debug("input file: {}".format(files))

        granule_id,_,_ = get_granule_id_from_filename(files, RE_NPP_str, iet_epoch, leapsec_dt_list)
        LOG.debug("granule_id = {}".format(granule_id))
        granule_id_from_files.append(granule_id)

    granule_id_from_files = list(set(granule_id_from_files))
    granule_id_from_files.sort()

    for granule_id in granule_id_from_files:
        LOG.debug("granule_id from files: {}".format(granule_id))

    # Loop through the input files and determine the dirs in which they are contained
    input_dirs_from_files = []
    for files in input_files:
        input_dirs_from_files.append(os.path.dirname(files))

    input_dirs_from_files = list(set(input_dirs_from_files))
    input_dirs_from_files.sort()

    for dirs in input_dirs_from_files:
        LOG.debug("input dirs from files: {}".format(dirs))

    # Have any of the input dirs from the file inputs already been covered by the dir inputs?
    # If yes, remove the dupes.
    LOG.debug("original input dirs from files: {}".format(input_dirs_from_files))
    for dirs in input_dirs_from_files:
        if dirs in input_dirs:
            LOG.debug("input dirs from files '{}' already in input_dirs".format(dirs,input_dirs))
            input_dirs_from_files = filter(lambda x: x != dirs, input_dirs_from_files)

    LOG.debug("filtered input dirs from files: {}".format(input_dirs_from_files))

    for dirs in input_dirs_from_files:
        for input_prefix in input_prefixes:
            input_glob = '{}*.h5'.format(input_prefix)
            input_glob = os.path.join(dirs, input_glob)
            LOG.debug("input glob is {}".format(input_glob))
            temp_input_files = glob.glob(input_glob)
            temp_input_files.sort()
            LOG.debug("temp_input_files {}".format(temp_input_files))

            for files in temp_input_files:

                granule_id, file_info, dt = get_granule_id_from_filename(files, RE_NPP_str, iet_epoch, 
                        leapsec_dt_list)

                LOG.debug("granule_id = {}".format(granule_id))

                if granule_id in granule_id_from_files:
                    kind_key = '{}{}'.format(file_info['kind'],file_info['band'])
                    try:
                        data_list[granule_id][kind_key] = file_info
                    except KeyError:
                        LOG.debug("Entry for granule ID {} does not yet exist, creating...".format(granule_id))
                        data_list[granule_id] = {}
                        data_list[granule_id][kind_key] = file_info

                    data_list[granule_id][kind_key]['file'] = files
                    data_list[granule_id][kind_key]['dt'] = dt

    return data_list

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

def cleanup(work_dir, objs_to_remove):
    """
    cleanup work directiory
    remove evething except the geocat products
    :param work_dir:
    """
    for file_obj in objs_to_remove:
        try:
            if os.path.isdir(file_obj) :
                LOG.info('Removing directory: {}'.format(file_obj))
                shutil.rmtree(file_obj)
            elif os.path.isfile(file_obj) :
                LOG.info('Removing file: {}'.format(file_obj))
                os.unlink(file_obj)
        except Exception:
            LOG.warn(traceback.format_exc())

def areas_to_L2(geo_home, cache_dir, work_dir, afire_options):
    """
    Take the obtained / created gvar areas and remap to standard projection
    """

    import cspp_ancil

    ret_val = 0

    attempted_runs = []
    successful_runs = []
    crashed_runs = []
    problem_runs = []

    # Get all of the required command line options for the required satellite...
    satellite_config = satellites.satellite_config[afire_options['satellite']]
    sat_obj = satellite_config(afire_options)

    area_files = sat_obj.generate_file_list()
    for area_file in area_files:
        LOG.info("area_file = {}".format(area_file))

    if area_files:

        for area_file in area_files:

            LOG.info(">>> Processing area file {}".format(area_file))
            LOG.debug("work_dir = {}".format(work_dir))

            # Set the input file
            afire_options['inputs'] = area_file
            sat_obj.set_input_paths(area_file)

            # Setting the input dir and file basename
            area_dir = sat_obj.input_dir
            area_file = sat_obj.input_filename

            LOG.info("area_dir = {}".format(area_dir))
            LOG.info("area_file = {}".format(area_file))

            attempted_runs.append(area_file)

            # Create the run dir for this area file
            log_idx = 0
            while True:
                run_dir = os.path.join(work_dir,"geocat_{}_run_{}".format(area_file,log_idx))
                if not os.path.exists(run_dir):
                    os.makedirs(run_dir)
                    break
                else:
                    log_idx += 1

            # Determine the datetime object for this input file...
            LOG.debug("getting the datetime object for {}".format(area_file))
            area_dt = sat_obj.datetime_from_input_filename(area_file)
            LOG.debug("datetime object for {} is : {}".format(area_file,area_dt))

            # Clean out product cache files that are too old.
            if not afire_options['preserve_cache']:
                LOG.info("Cleaning the temporal cache back {} hours...".format(afire_options['cache_window']))
                clean_hdf(afire_options['tmp_dir'],afire_options['cache_window'],area_dt)


            # Download and stage the required ancillary data for this area file
            LOG.info("Staging the required ancillary data...")
            rc_ancil = stage_ancillary(geo_home, cache_dir, run_dir, area_file, area_dt, afire_options)
            if rc_ancil != 0:
                LOG.warn('Ancillary retrieval failed for input file {}, proceeding...'.format(area_file))
                problem_runs.append(area_file)
                continue

            if not afire_options['ancillary_only']:
                rc,segment_rc,stitched_l1_rc,stitched_l2_rc = \
                        create_l2_products(run_dir, geo_home, afire_options, sat_obj)
            else:
                LOG.info("Ancillary ingest only, skipping geocat execution.")
                rc = 0
                segment_rc = [0]
                stitched_l1_rc = 0
                stitched_l2_rc = 0

            if rc != 0 :
                crashed_runs.append(area_file)
            elif np.sum(segment_rc) != 0 :
                problem_runs.append(area_file)
            elif stitched_l1_rc != 0 or stitched_l2_rc != 0 :
                problem_runs.append(area_file)
            else:
                successful_runs.append(area_file)
                if afire_options['docleanup']:
                    cleanup(work_dir, [run_dir])

            LOG.info(">>> Completed processing area file {}\n".format(area_file))


    else:
        LOG.error("Cannot find input area files.")

    attempted_runs  = list(set(attempted_runs))
    successful_runs = list(set(successful_runs))
    crashed_runs    = list(set(crashed_runs))
    problem_runs    = list(set(problem_runs))

    attempted_runs.sort()
    successful_runs.sort()
    crashed_runs.sort()
    problem_runs.sort()

    return attempted_runs,successful_runs,crashed_runs,problem_runs





def main():
    """
    The main method, returns 0 on success
    """

    # Read in the command line options
    args,work_dir,docleanup,logfile = argument_parser()

    # Check various paths and environment variables.
    try:

        afire_home = check_and_convert_env_var('CSPP_ACTIVE_FIRE_HOME')
        afire_ancil_path = check_and_convert_env_var('AFIRE_ANCIL_PATH')
        check_and_convert_path(None, os.path.join(afire_home, 'static_ancillary'), check_write=False)
        ver = check_existing_env_var('NOAA_AFIRE_VER')


    except CsppEnvironment as e:
        LOG.error( e.value )
        LOG.error('Installation error, Make sure all software components were installed.')
        return 2


    afire_options = {}
    afire_options['inputs'] = args.inputs
    afire_options['work_dir'] = os.path.abspath(args.work_dir)
    afire_options['ancillary_only'] = args.ancillary_only
    afire_options['num_cpu'] = args.num_cpu
    afire_options['docleanup'] = docleanup


    LOG.debug('afire home  {}'.format(afire_home))
    LOG.debug('work_dir  {}'.format(work_dir))
    LOG.debug('inputs {}'.format(args.inputs))


    # Create a list of dicts containing valid inputs
    afire_data_list =  generate_file_list(args.inputs)
    granule_id_list = afire_data_list.keys()
    granule_id_list.sort()
    for granule_id in granule_id_list:
        for file_kind in ['GMTCO','SVM05','SVM07', 'SVM11', 'SVM13', 'SVM15', 'SVM16']:
            LOG.info('afire_data_list[{}][{}] = {},{}'.format(
                granule_id,
                file_kind,
                afire_data_list[granule_id][file_kind]['dt'],
                afire_data_list[granule_id][file_kind]['file']
                )
            )
        LOG.info('')

    # Loop through the list of dicts, constructing valid command lines

    # Construct a list of jobs

    # Do cleanup work using return values

    return 0

    rc = 0
    try:

        attempted_runs,successful_runs,crashed_runs,problem_runs = areas_to_L2(
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
