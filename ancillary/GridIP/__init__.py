#!/usr/bin/env python
# encoding: utf-8
"""
This module re-implements the GridIP gridded ingest and granulation
in the Algorithm Development Package (ADL).

Created by Geoff Cureton on 2013-03-05.
Copyright (c) 2013 University of Wisconsin SSEC. All rights reserved.
"""

from LandWaterMask        import LandWaterMask
from QuarterlySurfaceType import QuarterlySurfaceType
from QstLwm               import QstLwm

classNames = {}
classNames['VIIRS-GridIP-VIIRS-Lwm-Mod-Gran'] = 'LandWaterMask'
classNames['VIIRS-GridIP-VIIRS-Qst-Mod-Gran'] = 'QuarterlySurfaceType'
classNames['VIIRS-GridIP-VIIRS-Qst-Lwm-Mod-Gran'] = 'QstLwm'
