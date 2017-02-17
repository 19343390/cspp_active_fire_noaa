#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import argparse
from datetime import datetime
import logging

import log_common
from utils import check_and_convert_path, create_dir

LOG = logging.getLogger(__name__)

def argument_parser():
    '''
    Method to encapsulate the option parsing and various setup tasks.
    '''

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
    help_strings['cache_dir'] = '''The directory where the granulated land water mask files are kept.'''
    help_strings['cache_window'] = '''Limit ancillary cache to hold no more that this number of hours
            preceding the target time. [default: %(default)s hours]'''    
    help_strings['preserve_cache'] = '''Do not flush old files from the ancillary cache. [default: %(default)s]'''
    help_strings['ancillary_only'] = '''Only process ancillary data, don't run Active Fires. [default: %(default)s]'''
    help_strings['num_cpu'] = """The number of CPUs to try and use. [default: %(default)s]"""
    help_strings['debug'] = '''Always retain intermediate files. [default: %(default)s]'''
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

    parser.add_argument('--cache_dir',
            dest='cache_dir',
            #default='.',
            help = help_strings['cache_dir']
            )

    parser.add_argument('--cache_window',
            dest='cache_window',
            action="store",
            type=float,
            default='6.',
            help = help_strings['cache_window'] if is_expert else argparse.SUPPRESS
            )

    parser.add_argument('--preserve_cache',
            dest='preserve_cache',
            action="store_true",
            default=False,
            help = help_strings['preserve_cache'] if is_expert else argparse.SUPPRESS
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
            #default=1,
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


    # Set up the logging
    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    level = levels[args.verbosity if args.verbosity < 4 else 3]

    # Create the work directory if it doesn't exist
    work_dir = os.path.abspath(os.path.expanduser(args.work_dir))
    work_dir = create_dir(work_dir)
    work_dir = check_and_convert_path("WORK_DIR", work_dir)

    dt = datetime.utcnow()
    timestamp = dt.isoformat()
    logname = "cspp_active_fire_noaa." + timestamp + ".log"
    logfile = os.path.join(work_dir, logname)
    log_common.configure_logging(level, FILE=logfile)

    LOG.debug('work directory : {}'.format(work_dir))

    docleanup = True
    if args.debug is True:
        docleanup = False

    return args, work_dir, docleanup, logfile
