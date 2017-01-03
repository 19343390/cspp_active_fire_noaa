#!/usr/bin/env python
# encoding: utf-8

import os
import sys
#import re, string
#import shutil
import argparse
import logging
#import time
#import glob
#import numpy as np
#import traceback
import datetime as dt
#from datetime import datetime, timedelta
#from cffi import FFI
#import fcntl

#ffi = FFI()
import log_common
#from geocat_interface import execution_time, create_l2_products
#import find_geocat_ancil
#import map_area_to_ancil
#import sync_ancillary
#import get_rap

#import satellites

from basics import check_and_convert_path
#from basics import link_files, check_and_convert_path, check_and_convert_env_var, \
        #check_existing_env_var, CsppEnvironment

#os.environ['TZ'] = 'UTC'

LOG = logging.getLogger(__name__)

def argument_parser():
    '''
    Method to encapsulate the option parsing and various setup tasks.
    '''

    desc = """Run NOAA Active Fire algorithm on VIIRS SDR files."""

    DARK_GRY   = "\033[0;90m"
    LGT_RED    = "\033[0;91m"
    LGT_GREEN  = "\033[0;92m"
    LGT_YELLOW = "\033[0;93m"
    LGT_BLUE   = "\033[0;94m"
    LGT_MAGENTA= "\033[0;95m"
    LGT_CYAN   = "\033[0;96m"
    WHITE      = "\033[0;97m"

    BLACK    = "\033[0;30m"
    RED      = "\033[0;31m"
    GREEN    = "\033[0;32m"
    YELLOW   = "\033[0;33m"
    BLUE     = "\033[0;34m"
    MAGENTA  = "\033[0;35m"
    CYAN     = "\033[0;36m"
    LIGHT_GRY= "\033[0;37m"
    BLUE     = "\033[0;37m"
    BLUE     = "\033[0;37m"
    NO_COLOR = "\033[0;39m"

    #desc = """{}Run {}GEOCAT {}level-2 {}algorithms {}on {}area {}files.{}""".format(
        #RED,LGT_RED,BLUE,LGT_BLUE,MAGENTA,CYAN,LGT_YELLOW,WHITE)
    desc = """Run NOAA Active Fire algorithm on VIIRS SDR files."""

    help_strings = {}
    help_strings['inputs'] = '''One or more input files or directories.'''
    help_strings['work_dir'] = '''The work directory.'''
    help_strings['ancillary_only'] = '''Only retrieve and process ancillary data, don't run geocat. [default: %(default)s]'''
    help_strings['num_cpu'] = """The number of CPUs to try and use. [default: %(default)s]"""

    help_strings['debug'] = '''always retain intermediate files. [default: %(default)s]'''
    help_strings['verbosity'] = '''each occurrence increases verbosity 1 level from
            ERROR: -v=WARNING -vv=INFO -vvv=DEBUG [default: %(default)s]'''
    help_strings['version'] = '''Print the CSPP Active Fires package version'''
    help_strings['expert'] = '''Display all help options, including the expert ones.'''


    is_expert = False
    if '--expert' in sys.argv:
        expert_index = sys.argv.index('--expert')
        sys.argv[expert_index] = '--help'
        is_expert = True
    elif '-x' in sys.argv  :
        expert_index = sys.argv.index('-x')
        sys.argv[expert_index] = '--help'
        is_expert = True
    else:
        pass


    parser = argparse.ArgumentParser(description=desc)


    # Mandatory/positional arguments

    parser.add_argument(
            action='store',
            dest='inputs',
            type=str,
            nargs='*',
            help = help_strings['inputs']
            )


    # Optional arguments

    parser.add_argument('-W', '--work-dir',
            dest='work_dir',
            metavar='work_dir',
            default='.',
            help = help_strings['work_dir']
            )

    parser.add_argument('--ancillary_only',
            dest='ancillary_only',
            action="store_true",
            default=False,
            help = help_strings['ancillary_only'] if is_expert else argparse.SUPPRESS
            )

    parser.add_argument('--num_cpu',
            action="store",
            dest="num_cpu",
            type=int,
            default=1,
            metavar=('NUM_CPU'),
            help = help_strings['num_cpu'] if is_expert else argparse.SUPPRESS
            )

    parser.add_argument('-d', '--debug',
            action="store_true",
            default=False,
            help = help_strings['debug']
            )

    parser.add_argument("-v", "--verbosity",
            dest='verbosity',
            action="count",
            default=2,
            help = help_strings['verbosity']
            )

    parser.add_argument('-V', '--version',
            action='version',
            version='''CSPP Active Fires v1.0beta''',
            help = help_strings['version']
            )

    parser.add_argument('-x','--expert',
            dest='is_expert',
            action="store_true",
            default=False,
            help = help_strings['expert']
            )

    args = parser.parse_args()

    ####################################################


    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    level = levels[args.verbosity if args.verbosity < 4 else 3]
    work_dir = check_and_convert_path("WORK_DIR", args.work_dir)
    d = dt.datetime.now()
    timestamp = d.isoformat()
    logname = "cspp_active_fire_noaa." + timestamp + ".log"
    logfile = os.path.join(work_dir, logname)
    log_common.configure_logging(level, FILE=logfile)

    #log_common.C_log_support(ffi)

    docleanup = True
    if args.debug is True:
        docleanup = False

    # Enforce any mutual exclusivity between various options.
    #view_mask = np.array([args.viewport!=None,
                          ##args.viewport_0!=None,
                          #args.viewport_xy!=None])
    #viewport_bitval = np.sum(view_mask * np.array([1,1]))
    #if viewport_bitval > 1:
        #parser.error("""Cannot specify more than one of '--viewport' or '--viewport_xy'.""")

    #if args.seg_rows==0 or args.seg_rows>max_segments:
        #parser.error("""Argument to '--line_segments' must be in the range [1..{}].""".format(max_segments))
    #if args.seg_cols==0 or args.seg_cols>max_segments:
        #parser.error("""Argument to '--element_segments' must be in the range [1..{}].""".format(max_segments))

    return args,work_dir,docleanup,logfile
