#!/usr/bin/env python
# encoding: utf-8
"""
active_file_interface.py


 * DESCRIPTION: This file contains routines that contruct a series of valid command line
 invocations for running the NOAA active fire algorithm. This includes building a dictionary of
 valid inputs from the input file/directory globs.

Created by Geoff Cureton on 2017-01-03.
Copyright (c) 2017 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
import re
import logging
from glob import glob
import numpy as np
from datetime import datetime

LOG = logging.getLogger('active_file_interface')


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


def get_granule_id_from_filename(filename, pattern, epoch, leapsec_dt_list):
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
    dt_string = "{}_{}".format(file_info['date'], file_info['start_time'])
    LOG.debug("dt_string = {}".format(dt_string))
    dt = datetime.strptime(dt_string, "%Y%m%d_%H%M%S%f")
    LOG.debug("dt = {}".format(dt))
    leap_seconds = int(get_leapseconds(leapsec_dt_list, dt))
    LOG.debug("leap_seconds = {}".format(leap_seconds))
    iet_time = int(((dt - epoch).total_seconds() + leap_seconds) * 1000000.)
    LOG.debug("iet_time = {}".format(iet_time))
    granule_id = get_granule_ID(iet_time)

    return granule_id, file_info, dt


def get_leapsec_table(leapsecond_dir):
    '''
    Read the IETTime.dat file containing the leap seconds since 1972, and save into a list of dicts.
    '''

    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    month_enum = {item: idx for idx, item in enumerate(months, start=1)}
    leapsec_filename = os.path.join(leapsecond_dir, 'IETTime.dat')
    try:
        leapsec_file = open(leapsec_filename, "ro")  # Open template file for reading
    except Exception, err:
        LOG.error("{}, aborting.".format(err))

    leapsec_dt_list = []
    for line in leapsec_file.readlines():
        line = line.replace("\n", "")
        fields = line.split(" ")
        fields = filter(lambda x: x != '', fields)
        year = int(fields[0])
        month = month_enum[fields[1]]
        day = int(fields[2])
        leap_secs = int(float(fields[6]))
        leap_dt = datetime(year, month, day)
        leapsec_dt_list.append({'dt': leap_dt, 'leapsecs': leap_secs})
    leapsec_file.close()

    return leapsec_dt_list


def get_leapseconds(leapsec_table, dt):
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


def generate_file_list(inputs, afire_options, full=False):
    '''
    Trawl through the files and directories given at the command line, pick out those matching the
    desired file types, and att them to a master list of raw data files. This list need not be
    sorted into time order.
    '''

    input_files = []

    for input in inputs:
        LOG.debug("bash glob input = {}".format(input))

    if full:
        input_files = list(set(inputs))
        input_files.sort()
        return input_files

    input_prefixes = ['GMTCO', 'SVM05', 'SVM07', 'SVM11', 'SVM13', 'SVM15', 'SVM16']

    input_dirs = []
    input_files = []

    # Sort the command line inputs into directory and file inputs...
    for input in inputs:
        input = os.path.abspath(os.path.expanduser(input))
        if os.path.isdir(input):
            # Input file glob is of form "/path/to/files"
            LOG.debug("Input {} is a directory containing files...".format(input))
            input_dirs.append(input)
        elif os.path.isfile(input):
            # Input file glob is of form "/path/to/files/goes13_1_2015_143_1745.input"
            LOG.debug("Input {} is a file.".format(input))
            input_files.append(input)

    input_dirs = list(set(input_dirs))
    input_dirs.sort()
    input_files = list(set(input_files))
    input_files.sort()

    for dirs in input_dirs:
        LOG.debug("input dirs {}".format(dirs))
    for files in input_files:
        LOG.debug("input files {}".format(files))

    # The re defining the fields of an NPP CDFCB-format filename
    RE_NPP_list = ['(?P<kind>[A-Z]+)(?P<band>[0-9]*)_',
                   '(?P<sat>[A-Za-z0-9]+)_', 'd(?P<date>\d+)_',
                   't(?P<start_time>\d+)_',
                   'e(?P<end_time>\d+)_b(?P<orbit>\d+)_',
                   'c(?P<created_time>\d+)_',
                   '(?P<site>[a-zA-Z0-9]+)_',
                   '(?P<domain>[a-zA-Z0-9]+)\.h5']
    RE_NPP_str = "".join(RE_NPP_list)

    # Get a table of the leap seconds
    iet_epoch = datetime(1958, 1, 1)
    LOG.debug("Epoch time: {}".format(iet_epoch))
    leapsec_dt_list = get_leapsec_table(afire_options['ancil_dir'])

    # Loop through the input dirs and record any desired files in any of these directories

    data_dict = {}
    for dirs in input_dirs:
        for input_prefix in input_prefixes:
            input_glob = '{}*.h5'.format(input_prefix)
            input_glob = os.path.join(dirs, input_glob)
            LOG.debug("input glob is {}".format(input_glob))
            temp_input_files = glob(input_glob)
            temp_input_files.sort()
            LOG.debug("temp_input_files {}".format(temp_input_files))

            for files in temp_input_files:

                granule_id, file_info, dt = get_granule_id_from_filename(files, RE_NPP_str,
                                                                         iet_epoch, leapsec_dt_list)

                LOG.debug("granule_id = {}".format(granule_id))

                kind_key = '{}{}'.format(file_info['kind'], file_info['band'])
                try:
                    data_dict[granule_id][kind_key] = file_info
                except KeyError:
                    LOG.debug("Entry for granule ID {} does not yet exist, creating...".format(
                        granule_id))
                    data_dict[granule_id] = {}
                    data_dict[granule_id][kind_key] = file_info

                data_dict[granule_id][kind_key]['file'] = files
                data_dict[granule_id][kind_key]['dt'] = dt

    # Loop through the input files, determine their dirs, and sweep up of the files in those dirs
    # that share granule ids with the command line inputs.

    # Get the granule ids of the files given in the input
    granule_id_from_files = []

    for files in input_files:
        LOG.debug("input file: {}".format(files))

        granule_id, _, _ = get_granule_id_from_filename(files, RE_NPP_str, iet_epoch,
                                                        leapsec_dt_list)
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
            LOG.debug("input dirs from files '{}' already in input_dirs".format(dirs, input_dirs))
            input_dirs_from_files = filter(lambda x: x != dirs, input_dirs_from_files)

    LOG.debug("filtered input dirs from files: {}".format(input_dirs_from_files))

    for dirs in input_dirs_from_files:
        for input_prefix in input_prefixes:
            input_glob = '{}*.h5'.format(input_prefix)
            input_glob = os.path.join(dirs, input_glob)
            LOG.debug("input glob is {}".format(input_glob))
            temp_input_files = glob(input_glob)
            temp_input_files.sort()
            LOG.debug("temp_input_files {}".format(temp_input_files))

            for files in temp_input_files:

                granule_id, file_info, dt = get_granule_id_from_filename(files, RE_NPP_str,
                                                                         iet_epoch, leapsec_dt_list)

                LOG.debug("granule_id = {}".format(granule_id))

                if granule_id in granule_id_from_files:
                    kind_key = '{}{}'.format(file_info['kind'], file_info['band'])
                    try:
                        data_dict[granule_id][kind_key] = file_info
                    except KeyError:
                        LOG.debug("Entry for granule ID {} does not yet exist, creating...".format(
                            granule_id))
                        data_dict[granule_id] = {}
                        data_dict[granule_id][kind_key] = file_info

                    data_dict[granule_id][kind_key]['file'] = files
                    data_dict[granule_id][kind_key]['dt'] = dt

    return data_dict


def construct_cmd_invocations(afire_data_dict):
    '''
    Take the list inputs, and construct the required command line invocations. Commands are of the
    form...

    vfire GMTCO.h5 SVM05.h5 SVM07.h5 SVM11.h5 SVM13.h5 SVM15.h5 SVM16.h5 \
        GRLWM_npp_d{}_t{}_e{}_b{}_ssec_dev.nc AFEDR_npp_d{}_t{}_e{}_b{}_cCTIME_ssec_dev.nc \
        metadata_id metadata_link time
    '''

    granule_id_list = afire_data_dict.keys()
    granule_id_list.sort()

    for granule_id in granule_id_list:

        # Construct the land water mask filename
        land_water_mask = 'GRLWM_npp_d{}_t{}_e{}_b{}_ssec_dev.nc'.format(
            afire_data_dict[granule_id]['GMTCO']['date'],
            afire_data_dict[granule_id]['GMTCO']['start_time'],
            afire_data_dict[granule_id]['GMTCO']['end_time'],
            afire_data_dict[granule_id]['GMTCO']['orbit']
        )
        afire_data_dict[granule_id]['GRLWM'] = {'file': land_water_mask}

        # Construct the output filename.
        afire_output_file = 'AFEDR_npp_d{}_t{}_e{}_b{}_cCTIME_ssec_dev.nc'.format(
            afire_data_dict[granule_id]['GMTCO']['date'],
            afire_data_dict[granule_id]['GMTCO']['start_time'],
            afire_data_dict[granule_id]['GMTCO']['end_time'],
            afire_data_dict[granule_id]['GMTCO']['orbit']
        )
        afire_data_dict[granule_id]['AFEDR'] = {'file': afire_output_file}

        # Construct the command line invocation. As the "vfire" binary is currently constructed,
        # The order of the inouts is important.
        afire_data_dict[granule_id]['cmd'] = './vfire_static {} {} {} {} {} {} {} {} {} '.format(
            os.path.basename(afire_data_dict[granule_id]['SVM13']['file']),
            os.path.basename(afire_data_dict[granule_id]['SVM15']['file']),
            os.path.basename(afire_data_dict[granule_id]['SVM16']['file']),
            os.path.basename(afire_data_dict[granule_id]['SVM05']['file']),
            os.path.basename(afire_data_dict[granule_id]['SVM07']['file']),
            os.path.basename(afire_data_dict[granule_id]['SVM11']['file']),
            os.path.basename(afire_data_dict[granule_id]['GMTCO']['file']),
            os.path.basename(afire_data_dict[granule_id]['GRLWM']['file']),
            os.path.basename(afire_data_dict[granule_id]['AFEDR']['file'])
        )
        afire_data_dict[granule_id]['cmd'] = '{} metadata_id metadata_link time'.format(
            afire_data_dict[granule_id]['cmd'])

        #afire_data_dict[granule_id]['cmd'] = 'sleep 0.5; echo "Executing {0:}"; exit 0'.format(
            #granule_id)
        #afire_data_dict[granule_id]['cmd'] = 'sleep 1; echo "Executing {}"; exit 0'.format(
            #granule_id)
        #afire_data_dict[granule_id]['cmd'] = ''.join(['echo "Executing {0:}...";',
                                                      #'python -c "import numpy as np; import time;',
                                                      #'t = 0.5 * np.random.randn() + 5.;',
                                                      #'time.sleep(t)";',
                                                      #'echo "Completed {0:}"; exit 0']).format(
                                                          #granule_id)

        # Construct the run directory name
        afire_data_dict[granule_id]['run_dir'] = 'NOAA_AFEDR_d{}_t{}_e{}_b{}_{}'.format(
            afire_data_dict[granule_id]['GMTCO']['date'],
            afire_data_dict[granule_id]['GMTCO']['start_time'],
            afire_data_dict[granule_id]['GMTCO']['end_time'],
            afire_data_dict[granule_id]['GMTCO']['orbit'],
            granule_id
        )

        afire_data_dict[granule_id]['granule_id'] = granule_id

    return afire_data_dict
