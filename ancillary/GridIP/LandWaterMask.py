#!/usr/bin/env python
# encoding: utf-8
"""
LandWaterMask.py

 * DESCRIPTION:  Class to granulate the DEM Land Water Mask data product

Created by Geoff Cureton on 2013-03-05.
Copyright (c) 2013 University of Wisconsin SSEC. All rights reserved.
Licensed under GNU GPLv3.
"""

import logging
from os import path
from time import time
from datetime import datetime
import numpy as np
from numpy import ma
import ctypes
from numpy.ctypeslib import ndpointer

import h5py
from netCDF4 import Dataset

from Utils import findDatelineCrossings
from Utils import index, find_lt, find_gt

LOG = logging.getLogger('LandWaterMask')


class LandWaterMask():

    def __init__(self, granule_dict, afire_options):
        self.collectionShortName = 'VIIRS-GridIP-VIIRS-Lwm-Mod-Gran'
        self.dataType = 'int8'
        self.sourceType = 'DEM'
        self.sourceList = ['']
        self.granule_dict = granule_dict
        self.afire_options = afire_options

        # Digital Elevation Model (DEM) land sea mask types
        self.DEM_list = ['DEM_SHALLOW_OCEAN', 'DEM_LAND', 'DEM_COASTLINE',
                         'DEM_SHALLOW_INLAND_WATER', 'DEM_EPHEMERAL_WATER',
                         'DEM_DEEP_INLAND_WATER', 'DEM_MOD_CONT_OCEAN', 'DEM_DEEP_OCEAN']
        self.DEM_dict = {
            'DEM_SHALLOW_OCEAN': 0,
            'DEM_LAND': 1,
            'DEM_COASTLINE': 2,
            'DEM_SHALLOW_INLAND_WATER': 3,
            'DEM_EPHEMERAL_WATER': 4,
            'DEM_DEEP_INLAND_WATER': 5,
            'DEM_MOD_CONT_OCEAN': 6,
            'DEM_DEEP_OCEAN': 7
        }

    def setGeolocationInfo(self):
        '''
        Populate this class instance with the geolocation data for a single granule
        '''

        try:
            # Open the geolocation file and get the latitude and longitude
            geo_filename = self.granule_dict['GMTCO']['file']
            geo_file_obj = h5py.File(geo_filename, 'r')

            # Get scan_mode to find any bad scans
            scanMode = geo_file_obj['/All_Data/VIIRS-MOD-GEO-TC_All/ModeScan'][:]
            badScanIdx = np.where(scanMode == 254)[0]
            if badScanIdx.size != 0:
                LOG.warning("Geolocation file {} has bad scans: {}".format(
                    geo_filename, badScanIdx))
                LOG.debug("Geolocation file {} has scans: {}".format(geo_filename, scanMode))

            # Detemine the min, max and range of the latitude and longitude,
            # taking care to exclude any fill values.

            latitude = geo_file_obj['/All_Data/VIIRS-MOD-GEO-TC_All/Latitude'][:]
            longitude = geo_file_obj['/All_Data/VIIRS-MOD-GEO-TC_All/Longitude'][:]

            latitude = ma.masked_less(latitude, -800.)
            latMin, latMax = np.min(latitude), np.max(latitude)
            latRange = latMax - latMin

            longitude = ma.masked_less(longitude, -800.)
            lonMin, lonMax = np.min(longitude), np.max(longitude)
            lonRange = lonMax - lonMin

            LOG.debug("min,max,range of latitide: {} {} {}".format(latMin, latMax, latRange))
            LOG.debug("min,max,range of longitude: {} {} {}".format(lonMin, lonMax, lonRange))

            geo_file_obj.close()

        except Exception, err:
            LOG.exception(err)
            LOG.exception("Problem opening geolocation file ({}), aborting.".format(geo_filename))
            geo_file_obj.close()
            return 1

        # Determine the latitude and longitude fill masks, so we can restore the
        # fill values after we have scaled...
        latMask = latitude.mask
        lonMask = longitude.mask

        # Restore fill values to masked pixels in geolocation

        latitude = ma.array(latitude, mask=latMask, fill_value=-999.)
        self.latitude = latitude.filled()

        longitude = ma.array(longitude, mask=lonMask, fill_value=-999.)
        self.longitude = longitude.filled()

        # Shift the longitudes to be between -180 and 180 degrees
        if lonMax > 180.:
            LOG.debug(
                "\nFinal min,max,range of longitude: {} {} {}".format(lonMin, lonMax, lonRange))
            dateLineIdx = np.where(longitude > 180.)
            LOG.debug("dateLineIdx = {}".format(dateLineIdx))
            longitude[dateLineIdx] -= 360.
            lonMax = np.max(ma.array(longitude, mask=lonMask))
            lonMin = np.min(ma.array(longitude, mask=lonMask))
            lonRange = lonMax - lonMin
            LOG.debug(
                "\nFinal min,max,range of longitude: {} {} {}".format(lonMin, lonMax, lonRange))

        # Record the corners, taking care to exclude any bad scans...
        nDetectors = 16
        firstGoodScan = np.where(scanMode <= 2)[0][0]
        lastGoodScan = np.where(scanMode <= 2)[0][-1]
        firstGoodRow = firstGoodScan * nDetectors
        lastGoodRow = lastGoodScan * nDetectors + nDetectors - 1

        latCrnList = [latitude[firstGoodRow, 0], latitude[firstGoodRow, -1],
                      latitude[lastGoodRow, 0], latitude[lastGoodRow, -1]]
        lonCrnList = [longitude[firstGoodRow, 0], longitude[firstGoodRow, -1],
                      longitude[lastGoodRow, 0], longitude[lastGoodRow, -1]]

        # Check for dateline/pole crossings
        num180Crossings = findDatelineCrossings(latCrnList, lonCrnList)
        LOG.debug("We have {} dateline crossings.".format(num180Crossings))

        # Copy the geolocation information to the class object
        self.latMin = latMin
        self.latMax = latMax
        self.latRange = latRange
        self.lonMin = lonMin
        self.lonMax = lonMax
        self.lonRange = lonRange
        self.scanMode = scanMode
        self.latitude = latitude
        self.longitude = longitude
        self.latCrnList = latCrnList
        self.lonCrnList = lonCrnList
        self.num180Crossings = num180Crossings

        return 0

    def subset(self):
        '''Subsets the LSM dataset to cover the required geolocation range.'''

        # Get the subset of DEM global dataset.

        DEM_dLat = 30. * (1. / 3600.)
        DEM_dLon = 30. * (1. / 3600.)

        DEM_fileName = path.join(self.afire_options['ancil_dir'],
                                 'dem30ARC_Global_LandWater_compressed.h5')
        self.sourceList.append(path.basename(DEM_fileName))

        try:
            # TODO : Use original HDF4 file which contains elevation and LWM.
            DEMobj = h5py.File(DEM_fileName, 'r')
            DEM_node = DEMobj['/demGRID/Data Fields/LandWater']
        except Exception, err:
            LOG.exception(err)
            LOG.exception("Problem opening DEM file ({}), aborting.".format(DEM_fileName))
            return 1

        try:
            DEM_gridLats = -1. * (np.arange(21600.) * DEM_dLat - 90.)
            DEM_gridLons = np.arange(43200.) * DEM_dLon - 180.

            LOG.debug("min,max DEM Grid Latitude values : {},{}".format(DEM_gridLats[0],
                                                                        DEM_gridLats[-1]))
            LOG.debug("min,max DEM Grid Longitude values : {},{}".format(DEM_gridLons[0],
                                                                         DEM_gridLons[-1]))

            latMin = self.latMin
            latMax = self.latMax
            lonMin = self.lonMin
            lonMax = self.lonMax

            DEM_latMask = np.equal((DEM_gridLats < (latMax + DEM_dLat)),
                                   (DEM_gridLats > (latMin - DEM_dLat)))
            DEM_lonMask = np.equal((DEM_gridLons < (lonMax + DEM_dLon)),
                                   (DEM_gridLons > (lonMin - DEM_dLon)))

            DEM_latIdx = np.where(DEM_latMask == True)[0]
            DEM_lonIdx = np.where(DEM_lonMask == True)[0]

            DEM_latMinIdx = DEM_latIdx[0]
            DEM_latMaxIdx = DEM_latIdx[-1]
            DEM_lonMinIdx = DEM_lonIdx[0]
            DEM_lonMaxIdx = DEM_lonIdx[-1]

            LOG.debug("DEM_latMinIdx = {}".format(DEM_latMinIdx))
            LOG.debug("DEM_latMaxIdx = {}".format(DEM_latMaxIdx))
            LOG.debug("DEM_lonMinIdx = {}".format(DEM_lonMinIdx))
            LOG.debug("DEM_lonMaxIdx = {}".format(DEM_lonMaxIdx))

            lat_subset = DEM_gridLats[DEM_latMinIdx:DEM_latMaxIdx + 1]
            self.gridLat = lat_subset

            if self.num180Crossings == 2:

                # We have a dateline crossing, so subset the positude and negative
                # longitude grids and sandwich them together.
                posLonCrn = np.min(ma.masked_less_equal(np.array(self.lonCrnList), 0.))
                negLonCrn = np.max(ma.masked_outside(np.array(self.lonCrnList), -800., 0.))
                posIdx = index(DEM_gridLons, find_lt(DEM_gridLons, posLonCrn))
                negIdx = index(DEM_gridLons, find_gt(DEM_gridLons, negLonCrn))

                posLons_subset = DEM_gridLons[posIdx:]
                negLons_subset = DEM_gridLons[:negIdx]
                lon_subset = np.concatenate((posLons_subset, negLons_subset))

                # Do the same with the DEM data
                posBlock = DEM_node[DEM_latMinIdx:DEM_latMaxIdx + 1, posIdx:]
                negBlock = DEM_node[DEM_latMinIdx:DEM_latMaxIdx + 1, :negIdx]
                DEM_subset = np.concatenate((posBlock, negBlock), axis=1)

            else:

                DEM_subset = DEM_node[DEM_latMinIdx:DEM_latMaxIdx + 1,
                                      DEM_lonMinIdx:DEM_lonMaxIdx + 1]
                lon_subset = DEM_gridLons[DEM_lonMinIdx:DEM_lonMaxIdx + 1]

            self.gridLon = lon_subset

            # Copy DEM data to the GridIP object
            self.gridData = DEM_subset.astype(self.dataType)

            del(DEM_node)
            DEMobj.close()

        except Exception, err:

            LOG.warning("EXCEPTION: {}".format(err))

            del(DEM_node)
            DEMobj.close()
            return 1

        return 0

    def _grid2Gran(self, dataLat, dataLon, gridData, gridLat, gridLon):
        '''Granulates a gridded dataset using an input geolocation'''

        nData = np.int64(dataLat.size)
        gridRows = np.int32(gridLat.shape[0])
        gridCols = np.int32(gridLat.shape[1])

        data = np.ones(np.shape(dataLat), dtype=np.float64) * 254.
        dataIdx = np.ones(np.shape(dataLat), dtype=np.int64) * -254

        libFile = path.join(self.afire_options['afire_home'],
                            'lib', 'libgriddingAndGranulation.so')
        LOG.debug("Gridding and granulation library file: {}".format(libFile))
        lib = ctypes.cdll.LoadLibrary(libFile)
        grid2gran = lib.grid2gran_nearest
        grid2gran.restype = None
        grid2gran.argtypes = [
            ndpointer(ctypes.c_double, ndim=1, shape=(nData), flags='C_CONTIGUOUS'),
            ndpointer(ctypes.c_double, ndim=1, shape=(nData), flags='C_CONTIGUOUS'),
            ndpointer(ctypes.c_double, ndim=1, shape=(nData), flags='C_CONTIGUOUS'),
            ctypes.c_int64,
            ndpointer(ctypes.c_double, ndim=2, shape=(gridRows, gridCols), flags='C_CONTIGUOUS'),
            ndpointer(ctypes.c_double, ndim=2, shape=(gridRows, gridCols), flags='C_CONTIGUOUS'),
            ndpointer(ctypes.c_double, ndim=2, shape=(gridRows, gridCols), flags='C_CONTIGUOUS'),
            ndpointer(ctypes.c_int64, ndim=1, shape=(nData), flags='C_CONTIGUOUS'),
            ctypes.c_int32,
            ctypes.c_int32
        ]

        '''
        int snapGrid_ctypes(double *lat,
                        double *lon,
                        double *data,
                        long nData,
                        double *gridLat,
                        double *gridLon,
                        double *gridData,
                        long *gridDataIdx,
                        int nGridRows,
                        int nGridCols
                        )
        '''

        try:
            LOG.debug("Calling C routine grid2gran()...")
            retVal = grid2gran(dataLat.astype('float64'),
                               dataLon.astype('float64'),
                               data,
                               nData,
                               gridLat,
                               gridLon,
                               gridData,
                               dataIdx,
                               gridRows,
                               gridCols)
            LOG.debug("Returning from C routine grid2gran() with retVal {}".format(retVal))
        except Exception, err:
            LOG.debug("There was a problem running C routine grid2gran()")
            LOG.warning("EXCEPTION: {}".format(err))

        return data, dataIdx

    def granulate(self):
        '''
        Granulates the GridIP DEM files.
        '''

        # Generate the lat and lon grids, and flip them and the data over latitude
        gridLon, gridLat = np.meshgrid(self.gridLon, self.gridLat[::-1])
        gridData = self.gridData[::-1, :]

        latitude = self.latitude
        longitude = self.longitude

        # If we have a dateline crossing, remove the longitude discontinuity
        # by adding 360 degrees to the negative longitudes.
        if self.num180Crossings == 2:
            gridLonNegIdx = np.where(gridLon < 0.)
            gridLon[gridLonNegIdx] += 360.
            longitudeNegIdx = np.where(longitude < 0.)
            longitude[longitudeNegIdx] += 360.

        LOG.debug("Granulating {} ..." .format(self.collectionShortName))
        LOG.debug("latitide,longitude shapes: {}, {}".format(str(latitude.shape),
                                                             str(longitude.shape)))
        LOG.debug("gridData.shape = {}".format(str(gridData.shape)))
        LOG.debug("gridLat.shape = {}".format(str(gridLat.shape)))
        LOG.debug("gridLon.shape = {}".format(str(gridLon.shape)))

        LOG.debug("min of gridData  = {}".format(np.min(gridData)))
        LOG.debug("max of gridData  = {}".format(np.max(gridData)))

        t1 = time()

        try:
            data, dataIdx = self._grid2Gran(np.ravel(latitude),
                                            np.ravel(longitude),
                                            gridData.astype(np.float64),
                                            gridLat.astype(np.float64),
                                            gridLon.astype(np.float64))
        except Exception, err:
            LOG.debug("There was a problem running  _grid2gran()")
            LOG.warning("EXCEPTION: {}".format(err))
            return 1

        t2 = time()
        elapsedTime = t2 - t1
        LOG.debug("Granulation of {} took {} seconds for {} points".format(
            self.granule_dict['granule_id'], elapsedTime, latitude.size))

        data = data.reshape(latitude.shape)
        dataIdx = dataIdx.reshape(latitude.shape)

        LOG.debug(
            "Shape of granulated {} data is {}".format(
                self.collectionShortName, np.shape(data)))
        LOG.debug(
            "Shape of granulated {} dataIdx is {}".format(
                self.collectionShortName, np.shape(dataIdx)))

        # Convert granulated data back to original type...
        self.data = data.astype(self.dataType)

        # Convert any "inland water" to "sea water"
        #shallowInlandWaterValue = self.DEM_dict['DEM_SHALLOW_INLAND_WATER']
        #shallowOceanValue = self.DEM_dict['DEM_SHALLOW_OCEAN']
        #deepInlandWaterValue = self.DEM_dict['DEM_DEEP_INLAND_WATER']
        #deepOceanValue = self.DEM_dict['DEM_DEEP_OCEAN']

        #shallowInlandWaterMask = ma.masked_equal(data,shallowInlandWaterValue).mask
        #shallowOceanMask = ma.masked_equal(data,shallowOceanValue).mask
        #deepInlandWaterMask = ma.masked_equal(data,deepInlandWaterValue).mask

        #totalWaterMask = shallowInlandWaterMask #+ shallowOceanMask + deepInlandWaterMask

        #data = ma.array(data,mask=totalWaterMask,fill_value=deepOceanValue)
        #data = data.filled()

        return 0

    def shipOutToFile(self, lwm_file, afire_options):
        '''
        Pass the current class instance to this Utils method to generate
        a blob/asc file pair from the input ancillary data object.
        '''

        # Get the geolocation fill values...
        geo_mask = ma.masked_less(self.latitude, -800.).mask

        # Get the granule time
        granule_dt = self.granule_dict['GMTCO']['dt']

        # Write the new data to the LWM template file
        LOG.debug("Opening LWM file {} for writing".format(lwm_file))
        file_obj = Dataset(lwm_file, "a", format="NETCDF4")
        try:
            # Get the latitude
            latitude_obj = file_obj['Latitude']
            latitude_obj[:] = self.latitude[:].astype('float32')

            # Get the longitude
            longitude_obj = file_obj['Longitude']
            longitude_obj[:] = self.longitude[:].astype('float32')

            # Get the Land Water Mask.
            lwm_obj = file_obj['LandMask']
            lwm_obj[:] = ma.array(self.data[:].astype('int8'), mask=geo_mask,
                                    fill_value=-128).filled()

            # Set some global attributes
            setattr(file_obj, 'History', datetime.utcnow().strftime("%a %b %d %H:%M:%S %Y UTC"))
            setattr(file_obj, 'Image_Date', granule_dt.strftime("%Y%j"))
            setattr(file_obj, 'Image_Time', granule_dt.strftime("%H%M%S"))
            setattr(file_obj, 'Source', 'dem30ARC; CSPP Active Fires version: {}'.format(
                afire_options['version']))

            # Close the file
            file_obj.close()

        except Exception, err:
            LOG.error("Writing to LWM file {} failed".format(lwm_file))
            LOG.debug("EXCEPTION: {}".format(err))
            return 1

        return 0
