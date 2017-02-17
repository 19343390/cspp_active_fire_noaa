#!/usr/bin/env python
# encoding: utf-8
"""
__init__.py

 * DESCRIPTION: This module re-implements the GridIP gridded ingest and granulation
in the Algorithm Development Package (ADL).

Created by Geoff Cureton on 2013-03-05.
Copyright (c) 2013 University of Wisconsin SSEC. All rights reserved.
"""

import logging

from LandWaterMask        import LandWaterMask

LOG = logging.getLogger('__init__')

classNames = {}
classNames['VIIRS-GridIP-VIIRS-Lwm-Mod-Gran'] = 'LandWaterMask'

