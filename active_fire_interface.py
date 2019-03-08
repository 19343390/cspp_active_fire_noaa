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
import sys
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import logging
#import time
import re
from glob import glob
import string
import numpy as np
import traceback
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

def get_leapsec_table(leapsecond_dir):
    '''
    Read the IETTime.dat file containing the leap seconds since 1972, and save into a list of dicts.
    '''

    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    month_enum = {item: idx for idx, item in enumerate(months, start=1)}
    leapsec_filename = pjoin(leapsecond_dir, 'IETTime.dat')
    try:
        leapsec_file = open(leapsec_filename, "r")  # Open template file for reading
    except IOError as err:
        LOG.error("{}, aborting.".format(err))

    leapsec_dt_list = []
    for line in leapsec_file.readlines():
        line = line.replace("\n", "")
        fields = line.split(" ")
        fields = list(filter(lambda x: x != '', fields))
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

def get_file_info(filename, afire_options, read_file=False):
    '''
    Computes a datetime object from "filename" using the regex "pattern", and determines the
    elapsed time since "epoch".
    '''
    # The re defining the fields of an NPP CDFCB-format filename
    RE_NPP_list = ['(?P<kind>[A-Z]+)(?P<band>[0-9]*)_',
                   '(?P<sat>[A-Za-z0-9]+)_', 'd(?P<date>\d+)_',
                   't(?P<start_time>\d+)_',
                   'e(?P<end_time>\d+)_b(?P<orbit>\d+)_',
                   'c(?P<created_time>\d+)_',
                   '(?P<site>[a-zA-Z0-9]+)_',
                   '(?P<domain>[a-zA-Z0-9]+)\.h5']
    pattern = "".join(RE_NPP_list)

    # Get a table of the leap seconds
    iet_epoch = datetime(1958, 1, 1)
    leapsec_dt_list = get_leapsec_table(afire_options['ancil_dir'])

    # Compile the regular expression for the filename...
    re_pattern = re.compile(pattern)

    # Get some information based on the filename
    file_basename = basename(filename)
    pattern_match = re_pattern.match(file_basename)
    if pattern_match is not None:
        file_info = dict(pattern_match.groupdict())
    else:
        return None, None, None, None

    # Determine the granule time info...
    dt_string = "{}_{}".format(file_info['date'], file_info['start_time'])
    dt = datetime.strptime(dt_string, "%Y%m%d_%H%M%S%f")
    leap_seconds = int(get_leapseconds(leapsec_dt_list, dt))

    is_aggregated = False
    agg_granule_IDs = []
    agg_iet_times = []

    if read_file:
        try:
            # Open the file and get the collection short name
            file_obj = h5py.File(filename, 'r')
            grp_obj = file_obj['/Data_Products']
            collection_short_name = list(grp_obj.keys())[0]

            # Determine whether this is an aggregated granule...
            agg_group_name = '/Data_Products/{0:}/{0:}_Aggr'.format(collection_short_name)
            grp_obj = file_obj[agg_group_name]
            num_grans = grp_obj.attrs['AggregateNumberGranules'][0][0]
            is_aggregated = True if num_grans > 1 else False

            # Get the IET and granule ID...
            LOG.debug('\t\tThere are {} granules in this file...'.format(num_grans))
            for granule in range(num_grans):
                gran_group_name = '/Data_Products/{0:}/{0:}_Gran_{1:}'.format(
                        collection_short_name, granule)
                grp_obj = file_obj[gran_group_name]
                agg_iet_times.append(grp_obj.attrs['N_Beginning_Time_IET'][0][0])
                agg_granule_IDs.append(grp_obj.attrs['N_Granule_ID'][0][0].decode())

            file_obj.close()
        except IOError as err:
            LOG.error("Reading of iet/granule_id failed for {}".format(filename))
            LOG.debug(traceback.format_exc())
            LOG.error("<<{}>>, aborting...".format(err))
            granule_id = []
        except Exception as err:
            LOG.error("Reading of iet/granule_id failed for {}".format(filename))
            LOG.debug(traceback.format_exc())
            LOG.error("<<{}>>, aborting...".format(err))
            file_obj.close()
            granule_id = []
    else:
        iet_time = int(((dt - iet_epoch).total_seconds() + leap_seconds) * 1000000.)
        granule_id = get_granule_ID(iet_time)

    return file_info, dt, is_aggregated, agg_granule_IDs, agg_iet_times


def inventory_files(input_files, afire_options, data_dict={}):
    '''
    Loop through the input files, and determine their granule ids
    '''

    # Set the required granule ID scheme...
    read_file = True

    if input_files == []:
        LOG.debug('\t\tNo input files')

    for input_file in input_files:
        LOG.debug("\t\tinput file: {}".format(input_file))

        file_info, dt, is_aggregated, granule_ids, iet_times = get_file_info(
                input_file, afire_options, read_file=read_file)

        if granule_ids == []:
            continue

        granule_id = sorted(granule_ids)[0]
        kind_key = '{}{}'.format(file_info['kind'], file_info['band'])

        LOG.debug("\t\tinput file granule_id = {}".format(granule_id))
        LOG.debug("\t\tinput file kind_key = {}".format(kind_key))

        try:
            # If an entry for this granule ID already exists
            if kind_key in data_dict[granule_id].keys():
                LOG.debug("\t\tdata_dict['{}']['{}'] has already been created!".format(granule_id, kind_key))
                LOG.debug("\t\t\tExisting entry: is_aggregated = {}, with {} granules".format(
                    data_dict[granule_id][kind_key]['is_aggregated'],
                    len(data_dict[granule_id][kind_key]['granule_ids'])))
                LOG.debug("\t\t\tNew entry: is_aggregated = {}, with {} granules".format(
                    is_aggregated, len(granule_ids)))

                if len(granule_ids) <= len(data_dict[granule_id][kind_key]['granule_ids']):
                    LOG.debug("\t\t\t...skipping this granule ID.")
                    continue
                else:
                    LOG.debug("\t\t\t...overwriting this granule ID.")

            data_dict[granule_id][kind_key] = file_info

        except KeyError:
            LOG.debug("\t\tEntry for granule ID {} does not yet exist, creating...".format(
                granule_id))
            data_dict[granule_id] = {}
            data_dict[granule_id][kind_key] = file_info

        data_dict[granule_id][kind_key]['file'] = input_file
        data_dict[granule_id][kind_key]['dt'] = dt
        data_dict[granule_id][kind_key]['is_aggregated'] = is_aggregated
        data_dict[granule_id][kind_key]['granule_id'] = granule_id
        data_dict[granule_id][kind_key]['granule_ids'] = granule_ids

    return data_dict

def inventory_dirs(input_dirs, afire_options):
    '''
    Loop through the input directories, find any files matching the required patterns, and
    determine their granule ids
    '''

    # Loop through the input dirs and record any desired files in any of these directories

    data_dict = {}

    if input_dirs == []:
        LOG.debug('\t\tNo input directories')

    input_prefixes = afire_options['input_prefixes']

    for dirs in input_dirs:
        LOG.debug("\tchecking directory for files: {}".format(dirs))
        for input_prefix in input_prefixes:
            input_glob = '{}*.h5'.format(input_prefix)
            input_glob = pjoin(dirs, input_glob)
            input_files = sorted(glob(input_glob))
            LOG.debug('')
            LOG.debug("\t\t>>> {} files in this dir:".format(basename(input_glob)))
            for input_file in input_files:
                LOG.debug('\t\t\t{}'.format(input_file))

            data_dict = inventory_files(input_files, afire_options, data_dict=data_dict)

    return data_dict

def show_dict(data_dict, dict_name='data_dict', leader=''):
    '''
    Print out the contents of a dictionary keyed by granule ID and then file "kind".
    '''
    if data_dict == {}:
        LOG.debug('{}>>> {} = {{}}'.format(leader, dict_name))
    else:
        for granule_id in sorted(data_dict.keys()):
            LOG.debug('{}>>> {}[{}]: '.format(leader, dict_name, granule_id))
            for kind in sorted(data_dict[granule_id].keys()):
                LOG.debug('{}\t[{}]: '.format(leader, kind))
                for key in data_dict[granule_id][kind].keys():
                    LOG.debug('{}\t\t{} :{} '.format(leader, key, data_dict[granule_id][kind][key]))

def generate_file_dict(inputs, afire_options, full=False):
    '''
    Trawl through the files and directories given at the command line, pick out those matching the
    desired file types, and construct a master dictionary addressed by granule ID and prefix type.
    '''

    input_files = []

    for input in inputs:
        LOG.debug("\tbash glob input = {}".format(input))

    if full:
        input_files = sorted(list(set(inputs)))
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
            LOG.debug("\tInput {} is a directory containing files...".format(input))
            input_dirs.append(input)
        elif isfile(input):
            # Input file glob is of form "/path/to/files/goes13_1_2015_143_1745.input"
            LOG.debug("\tInput {} is a file.".format(input))
            input_files.append(input)

    input_dirs = sorted(list(set(input_dirs)))
    input_files = sorted(list(set(input_files)))

    for dirs in input_dirs:
        LOG.debug("\tinput dirs {}".format(dirs))
    for files in input_files:
        LOG.debug("\tinput files {}".format(files))

    # Inventory the input files explicitly obtained from the command line.

    LOG.debug('')
    LOG.debug('>>> Checking explicit input files...')
    LOG.debug('')

    explicit_input_file_dict = inventory_files(input_files, afire_options)
    explicit_input_file_granule_ids = []
    LOG.debug('')

    show_dict(explicit_input_file_dict, dict_name='explicit_input_file_dict', leader='')

    for granule_id in explicit_input_file_dict.keys():
        for kind in explicit_input_file_dict[granule_id].keys():
            explicit_input_file_granule_ids += explicit_input_file_dict[granule_id][kind]['granule_ids']

    explicit_input_file_granule_ids = sorted(list(set(explicit_input_file_granule_ids)))
    LOG.debug('\t>>> explicit_input_file_granule_ids = {}: '.format(explicit_input_file_granule_ids))

    # Inventory the input files implicitly obtained by virtue of being in the same directory as the
    # explicitly obtained input files.

    LOG.debug('')
    LOG.debug('>>> Checking implicit input directories for any valid input files...')
    LOG.debug('')

    implicit_dirs = [[dirname(explicit_input_file_dict[granule_id][kind]['file'])
                        for kind in explicit_input_file_dict[granule_id].keys()]
                            for granule_id in explicit_input_file_dict.keys()]
    implicit_dirs =  sorted(list(set([x[0] for x in implicit_dirs])))
    LOG.debug('\timplicit dirs: {}'.format(implicit_dirs))
    LOG.debug('')
    implicit_input_dir_dict = inventory_dirs(implicit_dirs, afire_options)
    implicit_input_dir_granule_ids = []
    LOG.debug('')

    show_dict(implicit_input_dir_dict, dict_name='implicit_input_dir_dict', leader='')

    for granule_id in sorted(implicit_input_dir_dict.keys()):
        for kind in sorted(implicit_input_dir_dict[granule_id].keys()):
            implicit_input_dir_granule_ids += implicit_input_dir_dict[granule_id][kind]['granule_ids']

    implicit_input_dir_granule_ids = sorted(list(set(implicit_input_dir_granule_ids)))
    LOG.debug('\t>>> implicit_input_dir_granule_ids = {}: '.format(implicit_input_dir_granule_ids))

    # Inventory the input directories explicitly obtained from the command line.

    LOG.debug('')
    LOG.debug('>>> Checking explicit input directories for any valid input files...')
    LOG.debug('')

    explicit_dirs =  input_dirs

    LOG.debug('\texplicit dirs: {}'.format(explicit_dirs))
    LOG.debug('')
    explicit_input_dir_dict = inventory_dirs(explicit_dirs, afire_options)
    explicit_input_dir_granule_ids = []
    LOG.debug('')

    show_dict(explicit_input_dir_dict, dict_name='explicit_input_dir_dict', leader='')

    for granule_id in sorted(explicit_input_dir_dict.keys()):
        for kind in sorted(explicit_input_dir_dict[granule_id].keys()):
            explicit_input_dir_granule_ids += explicit_input_dir_dict[granule_id][kind]['granule_ids']

    explicit_input_dir_granule_ids = sorted(list(set(explicit_input_dir_granule_ids)))
    LOG.debug('\t>>> explicit_input_dir_granule_ids = {}: '.format(explicit_input_dir_granule_ids))

    LOG.debug('')
    LOG.debug('\t>>> explicit_input_file_granule_ids = {}: '.format(explicit_input_file_granule_ids))
    LOG.debug('\t>>> implicit_input_dir_granule_ids =  {}: '.format(implicit_input_dir_granule_ids))
    LOG.debug('\t>>> explicit_input_dir_granule_ids =  {}: '.format(explicit_input_dir_granule_ids))
    LOG.debug('')

    # Determine the allowed granule IDs...

    allowed_granule_ids = sorted(list(set(explicit_input_file_granule_ids
                                          + explicit_input_dir_granule_ids)))
    LOG.debug('\t>>> allowed_granule_ids = {}: '.format(allowed_granule_ids))
    LOG.debug('')

    # Find the aggregated files, and return the input dict with those files removed
    LOG.debug('\tChecking for aggregated files from explicit input files...')
    explicit_agg_input_files, explicit_input_file_dict = find_aggregated(explicit_input_file_dict)
    LOG.debug('\tChecking for aggregated files from implicit input files...')
    implicit_agg_input_dir_files, implicit_input_dir_dict = find_aggregated(implicit_input_dir_dict)
    LOG.debug('\tChecking for aggregated files from explicit input dirs...')
    explicit_agg_input_dir_files, explicit_input_dir_dict = find_aggregated(explicit_input_dir_dict)

    # Initialise the data dictionary...
    data_dict = {}
    data_dict.update(explicit_input_file_dict)
    data_dict.update(implicit_input_dir_dict)
    data_dict.update(explicit_input_dir_dict)

    # Handle the aggregated files
    agg_input_files = sorted(list(set(
        explicit_agg_input_files + implicit_agg_input_dir_files + explicit_agg_input_dir_files)))
    LOG.debug('')
    LOG.debug('\tagg_input_files = {}'.format(agg_input_files))

    if agg_input_files != []:

        # De-aggregate the aggregated files, and return the directory where the de-aggregated
        # files are...
        LOG.debug('\tDe-aggregating aggregated files...')
        afire_home = afire_options['afire_home']
        unagg_inputs_dir = unaggregate_inputs(afire_home, agg_input_files, afire_options)

        # Create a list of dicts containing valid inputs, from the de-aggregated files
        afire_unagg_data_dict = inventory_dirs([unagg_inputs_dir], afire_options)

        LOG.debug('\tDe-aggregated data dict...')
        show_dict(afire_unagg_data_dict, dict_name='afire_unagg_data_dict', leader='')

        LOG.debug('\tCombined data dicts (with aggregated files removed)...')
        show_dict(data_dict, dict_name='data_dict', leader='')

        # Update the main data_dict with the aggregated files
        #for granule_id in allowed_granule_ids:
            #data_dict[granule_id].update(afire_unagg_data_dict[granule_id])
        for granule_id in sorted(afire_unagg_data_dict.keys()):
            if granule_id in data_dict.keys():
                data_dict[granule_id].update(afire_unagg_data_dict[granule_id])
            else:
                data_dict[granule_id] = afire_unagg_data_dict[granule_id]

        LOG.debug('\tCombined data dicts (with de-aggregated files)...')
        show_dict(data_dict, dict_name='data_dict', leader='')

    # Remove disallowed granule IDs from the dicts
        LOG.debug('\t>>> Removing disallowed granule IDs from data_dict...')
    for granule_id in sorted(data_dict.keys()):
        if granule_id not in allowed_granule_ids:
            LOG.debug('\t\tRemoving granule ID {}'.format(granule_id))
            data_dict.pop(granule_id)

    return data_dict

def get_afire_inputs(inputs, afire_options):
    '''
    Take one or more inputs from the command line, and return a dictionary of inputs grouped by
    granule ID.
    '''

    afire_home = afire_options['afire_home']

    # Create a list of dicts containing valid inputs, which may include aggregated files
    LOG.debug('Creating master list of files...')
    afire_data_dict = generate_file_dict(inputs, afire_options)
    granule_id_list = sorted(afire_data_dict.keys())

    # Loop through the granule IDs and make sure that each one has a complete set of valid inputs.
    LOG.debug('Constructing valid sets of inputs...')
    bad_granule_id = []
    for granule_id in granule_id_list:
        LOG.debug('Checking granule_id {}...'.format(granule_id))
        missing_prefixes = []
        for prefix in sorted(afire_options['input_prefixes']):
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

    granule_id_list = sorted(afire_data_dict.keys())

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
            af_format_str = './{} -ndv' + ' {}' * 11
            afire_data_dict[granule_id]['cmd'] = af_format_str.format(
                vfire_exe,
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
