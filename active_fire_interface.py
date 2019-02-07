#!/usr/bin/env python
# encoding: utf-8
"""
active_fire_interface.py


 * DESCRIPTION: This file contains routines that contruct a series of valid command line
 invocations for running the NOAA active fire algorithm. This includes building a dictionary of
 valid inputs from the input file/directory globs.

Created by Geoff Cureton on 2017-01-03.
Copyright (c) 2017 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import logging
#import time
import re
from glob import glob
import string
import numpy as np
from datetime import datetime
import h5py

from unaggregate import find_aggregated, unaggregate_inputs

LOG = logging.getLogger('active_fire_interface')


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


def get_granule_id_from_file(filename, pattern, epoch, leapsec_dt_list, read_file=False):
    '''
    Computes a datetime object from "filename" using the regex "pattern", and determines the
    elapsed time since "epoch".
    '''
    # Compile the regular expression for the filename...
    re_pattern = re.compile(pattern)

    # Get some information based on the filename
    file_basename = basename(filename)
    LOG.debug("file_basename = {}".format(file_basename))
    pattern_match = re_pattern.match(file_basename)
    if pattern_match is not None:
        file_info = dict(pattern_match.groupdict())
    else:
        return None, None, None, None
    LOG.debug("file_info = {}".format(file_info))

    # Determine the granule time info...
    dt_string = "{}_{}".format(file_info['date'], file_info['start_time'])
    LOG.debug("dt_string = {}".format(dt_string))
    dt = datetime.strptime(dt_string, "%Y%m%d_%H%M%S%f")
    LOG.debug("dt = {}".format(dt))
    leap_seconds = int(get_leapseconds(leapsec_dt_list, dt))
    LOG.debug("leap_seconds = {}".format(leap_seconds))

    is_aggregated = False

    if read_file:
        try:
            # Open the file and get the collection short name
            file_obj = h5py.File(filename, 'r')
            grp_obj = file_obj['/Data_Products']
            collection_short_name = grp_obj.keys()[0]

            # Determine whether this is an aggregated granule...
            agg_group_name = '/Data_Products/{0:}/{0:}_Aggr'.format(collection_short_name)
            grp_obj = file_obj[agg_group_name]
            num_grans = grp_obj.attrs['AggregateNumberGranules'][0][0]
            is_aggregated = True if num_grans > 1 else False

            # Get the IET and granule ID...
            gran_group_name = '/Data_Products/{0:}/{0:}_Gran_0'.format(collection_short_name)
            grp_obj = file_obj[gran_group_name]
            iet_time = grp_obj.attrs['N_Beginning_Time_IET'][0][0]
            granule_id = grp_obj.attrs['N_Granule_ID'][0][0]

            file_obj.close()
        except IOError, err:
            LOG.error("Reading of iet/granule_id failed for {}".format(filename))
            LOG.error("{}, aborting...".format(err))
            granule_id = None
        except Exception, err:
            LOG.error("Reading of iet/granule_id failed for {}".format(filename))
            LOG.error("{}, aborting...".format(err))
            file_obj.close()
            granule_id = None
    else:
        iet_time = int(((dt - epoch).total_seconds() + leap_seconds) * 1000000.)
        LOG.debug("iet_time = {}".format(iet_time))
        granule_id = get_granule_ID(iet_time)

    LOG.debug("is_aggregated = {}".format(is_aggregated))

    return granule_id, file_info, dt, is_aggregated


def get_leapsec_table(leapsecond_dir):
    '''
    Read the IETTime.dat file containing the leap seconds since 1972, and save into a list of dicts.
    '''

    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    month_enum = {item: idx for idx, item in enumerate(months, start=1)}
    leapsec_filename = pjoin(leapsecond_dir, 'IETTime.dat')
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
    desired file types, and attach them to a master list of raw data files. This list need not be
    sorted into time order.
    '''

    input_files = []

    for input in inputs:
        LOG.debug("bash glob input = {}".format(input))

    if full:
        input_files = list(set(inputs))
        input_files.sort()
        return input_files

    m_band_prefixes = ['GMTCO', 'SVM05', 'SVM07', 'SVM11', 'SVM13', 'SVM15', 'SVM16']
    i_band_prefixes = ['GMTCO', 'GITCO', 'SVI01', 'SVI02', 'SVI03', 'SVI04', 'SVI05', 'SVM13', 'IVCDB']

    input_prefixes = i_band_prefixes if afire_options['i_band'] else m_band_prefixes

    afire_options['input_prefixes'] = input_prefixes

    input_dirs = []
    input_files = []

    # Sort the command line inputs into directory and file inputs...
    for input in inputs:
        input = abspath(os.path.expanduser(input))
        if isdir(input):
            # Input file glob is of form "/path/to/files"
            LOG.debug("Input {} is a directory containing files...".format(input))
            input_dirs.append(input)
        elif isfile(input):
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

    # Set the required granule ID scheme...
    read_file = True

    # Loop through the input dirs and record any desired files in any of these directories

    data_dict = {}
    for dirs in input_dirs:
        for input_prefix in input_prefixes:
            input_glob = '{}*.h5'.format(input_prefix)
            input_glob = pjoin(dirs, input_glob)
            LOG.debug("input glob is {}".format(input_glob))
            temp_input_files = glob(input_glob)
            temp_input_files.sort()
            LOG.debug("temp_input_files {}".format(temp_input_files))

            for temp_input_file in temp_input_files:

                granule_id, file_info, dt, is_aggregated = get_granule_id_from_file(
                    temp_input_file, RE_NPP_str,
                    iet_epoch, leapsec_dt_list, read_file=read_file)

                if granule_id is None:
                    continue

                LOG.debug("granule_id = {}".format(granule_id))

                kind_key = '{}{}'.format(file_info['kind'], file_info['band'])
                try:
                    data_dict[granule_id][kind_key] = file_info
                except KeyError:
                    LOG.debug("Entry for granule ID {} does not yet exist, creating...".format(
                        granule_id))
                    data_dict[granule_id] = {}
                    data_dict[granule_id][kind_key] = file_info

                data_dict[granule_id][kind_key]['file'] = temp_input_file
                data_dict[granule_id][kind_key]['dt'] = dt
                data_dict[granule_id][kind_key]['is_aggregated'] = is_aggregated

    # Loop through the input files, determine their dirs, and sweep up the files in those dirs
    # that share granule ids with the command line inputs.

    # Get the granule ids of the files given in the input
    granule_id_from_files = []

    for input_file in input_files:
        LOG.debug("input file: {}".format(input_file))

        granule_id, _, _, is_aggregated = get_granule_id_from_file(
            input_file, RE_NPP_str, iet_epoch, leapsec_dt_list, read_file=read_file)

        if granule_id is None:
            continue

        LOG.debug("granule_id = {}".format(granule_id))
        granule_id_from_files.append(granule_id)

    granule_id_from_files = list(set(granule_id_from_files))
    granule_id_from_files.sort()

    for granule_id in granule_id_from_files:
        LOG.debug("granule_id from files: {}".format(granule_id))

    # Loop through the input files and determine the dirs in which they are contained
    input_dirs_from_files = []
    for input_file in input_files:
        input_dirs_from_files.append(dirname(input_file))

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
            input_glob = pjoin(dirs, input_glob)
            LOG.debug("input glob is {}".format(input_glob))
            temp_input_files = glob(input_glob)
            temp_input_files.sort()
            LOG.debug("temp_input_files {}".format(temp_input_files))

            for temp_input_file in temp_input_files:

                granule_id, file_info, dt, is_aggregated = get_granule_id_from_file(
                    temp_input_file, RE_NPP_str, iet_epoch, leapsec_dt_list, read_file=read_file)

                if granule_id is None:
                    continue

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

                    data_dict[granule_id][kind_key]['file'] = temp_input_file
                    data_dict[granule_id][kind_key]['dt'] = dt
                    data_dict[granule_id][kind_key]['is_aggregated'] = is_aggregated

    return data_dict


def get_afire_inputs(inputs, afire_options):
    '''
    Take one or more inputs from the command line, and return a dictionary of inputs grouped by
    granule ID.
    '''

    afire_home = afire_options['afire_home']

    # Create a list of dicts containing valid inputs, which may include aggregated files
    afire_data_dict = generate_file_list(inputs, afire_options)
    granule_id_list = afire_data_dict.keys()
    granule_id_list.sort()

    # Find the aggregated files...
    agg_input_files = find_aggregated(afire_data_dict)
    granule_id_list = afire_data_dict.keys()
    granule_id_list.sort()
    LOG.debug('agg_input_files = {}'.format(agg_input_files))

    if agg_input_files != []:

        # Unaggregate the aggregated files, and return the directory where the unaggregated
        # files are...
        unagg_inputs_dir = unaggregate_inputs(afire_home, agg_input_files, afire_options)

        afire_unagg_data_dict = generate_file_list([unagg_inputs_dir], afire_options)
        granule_id_unagg_list = afire_unagg_data_dict.keys()
        granule_id_unagg_list.sort()

        # Combine the two data dicts...
        afire_data_dict.update(afire_unagg_data_dict)
        granule_id_list = afire_data_dict.keys()
        granule_id_list.sort()

    # Loop through the granule IDs and make sure that each one has a complete set of valid inputs.
    bad_granule_id = []
    for granule_id in granule_id_list:
        LOG.debug('Checking granule_id {}...'.format(granule_id))
        missing_prefixes = []
        for prefix in afire_options['input_prefixes']:
            LOG.debug('\tChecking prefix {}...'.format(prefix))
            try:
                LOG.debug('\t\tafire_data_dict["{}"]["{}"] = {}'.format(
                    granule_id, prefix, basename(afire_data_dict[granule_id][prefix]['file'])))
            except KeyError:
                LOG.debug("\t\tInput prefix {} not present...".format(prefix))
                missing_prefixes.append(prefix)

        if missing_prefixes != []:
            LOG.warn("Granule ID {} is missing the prefixes {}, removing this granule ID..."
                    .format(granule_id, ', '.join(missing_prefixes)))
            bad_granule_id.append(granule_id)

    for granule_id in bad_granule_id:
        granule_id_list.pop(granule_id_list.index(granule_id))
        afire_data_dict.pop(granule_id)

    return afire_data_dict, granule_id_list


def construct_cmd_invocations(afire_data_dict, afire_options):
    '''
    Take the list inputs, and construct the required command line invocations. Commands are of the
    form...

    vfire_m GMTCO.h5 SVM05.h5 SVM07.h5 SVM11.h5 SVM13.h5 SVM15.h5 SVM16.h5 \
        GRLWM_npp_d{}_t{}_e{}_b{}_cspp_dev.nc AFEDR_npp_d{}_t{}_e{}_b{}_cCTIME_cspp_dev.nc \
        metadata_id metadata_link time

    vfire_i GITCO.h5 SVI01.h5 SVI02.h5 SVI03.h5 SVI04.h5 SVI05.h5 SVM13.h5 IVCDB.h5 \
        GRLWM_npp_d{}_t{}_e{}_b{}_cspp_dev.nc AFEDR_npp_d{}_t{}_e{}_b{}_cCTIME_cspp_dev.nc \
        metadata_id metadata_link time

    The I-band AF still requires the M-band geolocation (GMTCO) to granulate the LWM, which is still
    750m resolution.
    '''

    granule_id_list = afire_data_dict.keys()
    granule_id_list.sort()

    geo_prefix = 'GITCO' if afire_options['i_band'] else 'GMTCO'
    lwm_prefix = 'GRLWM'
    af_prefix = 'AFIMG' if afire_options['i_band'] else 'AFMOD'
    vfire_exe = 'vfire375_noaa_static' if afire_options['i_band'] else 'vfire_hdf5_static'

    afire_options['vfire_exe'] = vfire_exe

    for granule_id in granule_id_list:

        creation_dt = datetime.utcnow()

        # Construct the land water mask filename
        '''
        AF-LAND_MASK_NASA_1KM_npp_s201808072032197_e201808072033439_c201808072126320.nc
        '''
        lwm_start_time = '{}{}'.format(
            afire_data_dict[granule_id][geo_prefix]['date'],
            afire_data_dict[granule_id][geo_prefix]['start_time']
            )
        lwm_end_time = '{}{}'.format(
            afire_data_dict[granule_id][geo_prefix]['date'],
            afire_data_dict[granule_id][geo_prefix]['end_time']
            )

        land_water_mask = 'AF-LAND_MASK_NASA_1KM_{}_s{}_e{}.nc'.format(
            afire_data_dict[granule_id][geo_prefix]['sat'],
            lwm_start_time,
            lwm_end_time,
            #creation_dt.strftime("%Y%m%d%H%M%S%f")
        )
        afire_data_dict[granule_id]['GRLWM'] = {'file': land_water_mask}

        # Construct the output filename.
        afire_output_file = '{}_{}_d{}_t{}_e{}_b{}_c{}_cspp_dev.nc'.format(
            af_prefix,
            afire_data_dict[granule_id][geo_prefix]['sat'],
            afire_data_dict[granule_id][geo_prefix]['date'],
            afire_data_dict[granule_id][geo_prefix]['start_time'],
            afire_data_dict[granule_id][geo_prefix]['end_time'],
            afire_data_dict[granule_id][geo_prefix]['orbit'],
            creation_dt.strftime("%Y%m%d%H%M%S%f")
        )
        afire_output_txt_file = '{}.txt'.format(splitext(afire_output_file)[0])

        afire_data_dict[granule_id]['AFEDR'] = {
            'file': afire_output_file,
            'txt': afire_output_txt_file
        }

        # Construct the command line invocation. As the "vfire" binary is currently constructed,
        # The order of the inputs is important.
        if afire_options['i_band']:
            #try:
            af_format_str = './{} -a {} -ndv' + ' {}' * 11
            afire_data_dict[granule_id]['cmd'] = af_format_str.format(
                vfire_exe,
                basename(afire_data_dict[granule_id]['AFEDR']['txt']),
                basename(afire_data_dict[granule_id]['SVI01']['file']),
                basename(afire_data_dict[granule_id]['SVI02']['file']),
                basename(afire_data_dict[granule_id]['SVI03']['file']),
                basename(afire_data_dict[granule_id]['SVI04']['file']),
                basename(afire_data_dict[granule_id]['SVI05']['file']),
                basename(afire_data_dict[granule_id]['SVM13']['file']),
                basename(afire_data_dict[granule_id]['IVCDB']['file']),
                basename(afire_data_dict[granule_id]['GITCO']['file']),
                basename(afire_data_dict[granule_id]['GRLWM']['file']),
                'PersistentWaterFireRef.txt',
                basename(afire_data_dict[granule_id]['AFEDR']['file'])
            )
            #except KeyError:

        else:
            af_format_str = './{}' + ' {}' * 10
            afire_data_dict[granule_id]['cmd'] = af_format_str.format(
                vfire_exe,
                basename(afire_data_dict[granule_id]['SVM13']['file']),
                basename(afire_data_dict[granule_id]['SVM15']['file']),
                basename(afire_data_dict[granule_id]['SVM16']['file']),
                basename(afire_data_dict[granule_id]['SVM05']['file']),
                basename(afire_data_dict[granule_id]['SVM07']['file']),
                basename(afire_data_dict[granule_id]['SVM11']['file']),
                basename(afire_data_dict[granule_id]['GMTCO']['file']),
                basename(afire_data_dict[granule_id]['GRLWM']['file']),
                basename(afire_data_dict[granule_id]['AFEDR']['file']),
                'metadata_id metadata_link time'
            )

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
        afire_data_dict[granule_id]['run_dir'] = 'NOAA_{}_{}_d{}_t{}_e{}_b{}_{}'.format(
            af_prefix,
            afire_data_dict[granule_id][geo_prefix]['sat'],
            afire_data_dict[granule_id][geo_prefix]['date'],
            afire_data_dict[granule_id][geo_prefix]['start_time'],
            afire_data_dict[granule_id][geo_prefix]['end_time'],
            afire_data_dict[granule_id][geo_prefix]['orbit'],
            granule_id
        )

        afire_data_dict[granule_id]['granule_id'] = granule_id
        afire_data_dict[granule_id]['creation_dt'] = creation_dt

    return afire_data_dict
