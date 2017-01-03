#!/usr/bin/env python
# encoding: utf-8
"""
$Id: adl_common.py 1931 2014-03-17 18:43:26Z kathys $

Purpose: Common routines for ADL XDR handling and ancillary data caching.

Requires: adl_asc

Created Oct 2011 by R.K.Garcia <rayg@ssec.wisc.edu>
Copyright (c) 2011 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os, sys, logging
import basics

import __main__

#from cffi import FFI
#ffi = FFI()

ffi = None

LOG = logging.getLogger(__name__)

logging_configured = False


# ref: http://stackoverflow.com/questions/1383254/logging-streamhandler-and-standard-streams
class SingleLevelFilter(logging.Filter):
    def __init__(self, passlevels, reject):
        self.passlevels = set(passlevels)
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return (record.levelno not in self.passlevels)
        else:
            return (record.levelno in self.passlevels)


def configure_logging(level=logging.WARNING, FILE=None):
    """
    route logging INFO and DEBUG to stdout instead of stderr, affects entire application
    """

    global logging_configured
    # create a formatter to be used across everything
    #fm = logging.Formatter('%(levelname)s:%(name)s:%(msg)s') # [%(filename)s:%(lineno)d]')

    ### Orig
    #fm = logging.Formatter('(%(levelname)s):%(filename)s:%(funcName)s:%(lineno)d:%(message)s')

    if level == logging.DEBUG :
        fm = logging.Formatter('%(asctime)s.%(msecs)03d (%(levelname)s) : %(filename)s : %(funcName)s : %(lineno)d:%(message)s',\
                datefmt='%Y-%m-%d %H:%M:%S')
    else:
        fm = logging.Formatter('%(asctime)s.%(msecs)03d (%(levelname)s) : %(message)s',\
                datefmt='%Y-%m-%d %H:%M:%S')

    rootLogger = logging.getLogger()

    # set up the default logging
    if logging_configured == False:
        logging_configured = True

        # create a handler which routes info and debug to stdout with std formatting
        h1 = logging.StreamHandler(sys.stdout)
        f1 = SingleLevelFilter([logging.INFO, logging.DEBUG], False)
        h1.addFilter(f1)
        h1.setFormatter(fm)

        # create a second stream handler which sends everything else to stderr with std formatting
        h2 = logging.StreamHandler(sys.stderr)
        f2 = SingleLevelFilter([logging.INFO, logging.DEBUG], True)
        h2.addFilter(f2)
        h2.setFormatter(fm)
        rootLogger.addHandler(h1)
        rootLogger.addHandler(h2)

    h3 = None
    if FILE is not None:
        work_dir = os.path.dirname(FILE)
        basics.check_and_convert_path("WORKDIR", work_dir, check_write=True)
        h3 = logging.FileHandler(filename=FILE)
        #        f3 = SingleLevelFilter([logging.INFO, logging.DEBUG], False)
        #        h3.addFilter(f3)
        h3.setFormatter(fm)
        rootLogger.addHandler(h3)

    rootLogger.setLevel(level)


def status_line(status):
    """
    Put out a special status line
    """


    LOG.info('\n                 ( %s )\n'%status)


def log_from_C(in_type, in_msg):
    type = ffi.string(in_type)
    msg = ffi.string(in_msg)

    if type == 'INFO':
        LOG.info(msg)
    elif type == 'WARN':
        LOG.warn(msg)
    elif type == 'ERROR':
        LOG.error(msg)
    elif type == 'DEBUG':
        LOG.debug(msg)
    else:
        LOG.error('Bad TYPE %s %s' % (type, msg))

    return int(0)


log_callback = None
log_lib = None


def C_log_support(ffi_in):
    """
    Initalizer for C callbacks
    """

    global ffi
    global log_callback
    global log_lib

    ffi = ffi_in
    ffi.cdef("""
    void log_from_C(char * type, char * message );
    void set_log( int (*callback)(char*,char*) );
    int LOG_info( char *message);
    int LOG_warn( char *message);
    int LOG_debug( char *message);
    int LOG_error( char *message);
    """ )

    log_lib = ffi.dlopen("liblog_common_cb.so")
    log_callback = ffi.callback("int(char*,char *)", log_from_C)
    log_lib.set_log(log_callback)


def _test_logging():
    LOG.debug('debug message')
    LOG.info('info message')
    LOG.warning('warning message')
    LOG.error('error message')
    LOG.critical('critical message')


def test_C_callbacks():
    message = 'error'
    arg1 = ffi.new("char[]", message)
    log_lib.LOG_error(arg1)

    message = 'warn'
    arg2 = ffi.new("char[]", message)
    log_lib.LOG_warn(arg2)

    message = 'info'
    arg3 = ffi.new("char[]", message)
    log_lib.LOG_info(arg3)

    message = 'debug'
    arg4 = ffi.new("char[]", message)
    log_lib.LOG_debug(arg4)


if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG) we don't want basicConfig anymore
    configure_logging(level=logging.DEBUG, FILE="./testlog.log")
    _test_logging()

    C_log_support()
#    test_C_callbacks()

