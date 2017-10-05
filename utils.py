#!/usr/bin/env python
# encoding: utf-8
'''
utils.py

 * DESCRIPTION: This is the collection of utilities for parsing text, running external processes and
 binaries, checking environments and whatever other mundane tasks aren't specific to this project.

Created Oct 2011 by R.K.Garcia <rayg@ssec.wisc.edu>
Copyright (c) 2011 University of Wisconsin Regents.
Licensed under GNU GPLv3.
'''

import os
import sys
import re
import string
import logging
import log_common
import traceback
import time
from glob import glob
import types
import fileinput
import shutil
from copy import copy
import uuid
from subprocess import Popen, CalledProcessError, call, PIPE
from datetime import datetime, timedelta
from threading import Thread
from Queue import Queue, Empty


LOG = logging.getLogger(__name__)

PROFILING_ENABLED = os.environ.get('CSPP_PROFILE', None) is not None
STRACE_ENABLED = os.environ.get('CSPP_STRACE', None) is not None


def split_search_path(s):
    '''
    Break a colon-separated list of directories into a list of directories, else empty-list
    '''
    if not s:
        return []

    back_list = []
    for path in s.split(':'):
        back_list.append(os.path.abspath(path))

    return back_list


def _replaceAll(intputfile, searchExp, replaceExp):
    '''
    Replace all instances of 'searchExp' with 'replaceExp' in 'intputfile'
    '''
    for line in fileinput.input(intputfile, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        sys.stdout.write(line)
    fileinput.close()


def cleanup(objs_to_remove):
    """
    cleanup directories / files
    """
    for file_obj in objs_to_remove:
        try:
            if os.path.isdir(file_obj):
                LOG.debug('Removing directory: {}'.format(file_obj))
                shutil.rmtree(file_obj)
            elif os.path.isfile(file_obj):
                LOG.debug('Removing file: {}'.format(file_obj))
                os.unlink(file_obj)
        except Exception:
            LOG.warn("Unable to remove {}".format(file_obj))
            LOG.debug(traceback.format_exc())


class AscLineParser(object):
    def time_range(self, ascLine):
        '''
        :param ascLine:
        :return:
        '''
        day, time = self.extract_time_range_tokens(ascLine)
        return self.time_from_tokens(day, time)

    def extract_time_range_tokens(self, ascLine):
        return ascLine.split('"')[3:4][0].split(' ')

    def time_from_tokens(self, day, time):
        dt = datetime.strptime(day + time, '%Y-%m-%d%H:%M:%S.%f')
        return dt


def getURID(URID_timeObj=None):
    '''
    Create a new URID to be used in making the asc filenames
    '''

    URID_dict = {}

    if URID_timeObj is None:
        URID_timeObj = datetime.utcnow()

    creationDateStr = URID_timeObj.strftime("%Y-%m-%d %H:%M:%S.%f")
    creationDate_nousecStr = URID_timeObj.strftime("%Y-%m-%d %H:%M:%S.000000")

    tv_sec = int(URID_timeObj.strftime("%s"))
    tv_usec = int(URID_timeObj.strftime("%f"))
    hostId_ = uuid.getnode()
    thisAddress = id(URID_timeObj)

    l = tv_sec + tv_usec + hostId_ + thisAddress

    URID = '-'.join(('{0:08x}'.format(tv_sec)[:8],
                     '{0:05x}'.format(tv_usec)[:5],
                     '{0:08x}'.format(hostId_)[:8],
                     '{0:08x}'.format(l)[:8]))

    URID_dict['creationDateStr'] = creationDateStr
    URID_dict['creationDate_nousecStr'] = creationDate_nousecStr
    URID_dict['tv_sec'] = tv_sec
    URID_dict['tv_usec'] = tv_usec
    URID_dict['hostId_'] = hostId_
    URID_dict['thisAddress'] = thisAddress
    URID_dict['URID'] = URID

    return URID_dict


def link_files(dest_path, files):
    '''
    Link ancillary files into a destination directory.
    '''
    files_linked = 0
    for src_file in files:
        src = os.path.basename(src_file)
        dest_file = os.path.join(dest_path, src)
        if not os.path.exists(dest_file):
            LOG.debug("Link {0} -> {1}".format(src_file, dest_file))
            os.symlink(src_file, dest_file)
            files_linked += 1
        else:
            LOG.warn('link already exists: {}'.format(dest_file))
            files_linked += 1
    return files_linked


class CsppEnvironment(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def check_and_convert_path(key, a_path, check_write=False):
    '''
    Make sure the path or paths specified exist
    Return the path or list of absolute paths that exist
    '''
    abs_locations = []
    if ":" in a_path:
        paths = a_path.split(":")
    elif isinstance(a_path, types.StringTypes):
        paths = [a_path]
    else:
        paths = a_path

    for path in paths:
        if not os.path.exists(path):
            if key:
                msg = "Environment variable {} refers to a path that does not exist.  {}={}".format(
                    key, key, path)
            else:
                msg = "Required path {} does not exist.".format(path)

            raise CsppEnvironment(msg)
            #sys.exit(2)
            return None
        else:
            LOG.debug("Found: {} at {} {}".format(key, path, os.path.abspath(path)))
            abs_locations.append(os.path.abspath(path))

        if check_write:
            if not os.access(path, os.W_OK):
                msg = "Path exists but is not writable {}={}".format(key, path)
                raise CsppEnvironment(msg)

    # return a string if only one and an array if more
    if len(abs_locations) == 1:
        return abs_locations[0]
    else:
        #return abs_locations
        # return a :-joined string for use in an env variable
        return ':'.join(abs_locations)


def check_existing_env_var(varname, default_value=None, flag_warn=False):
    '''
    If variable exists then use value, otherwise use default
    '''
    value = None
    if varname in os.environ:
        value = os.environ.get(varname)
    else:
        if default_value is not None:
            value = default_value
        else:
            if flag_warn:
                LOG.warn("{} is not set, please update environment and re-try".format(varname))
                LOG.warn("Environment variable missing. {}".format(varname))
            else:
                LOG.debug("{} is not set, please update environment and re-try".format(varname))
                LOG.debug("Environment variable missing. {}".format(varname))

    return value


def check_and_convert_env_var(varname, check_write=False, default_value=None, flag_warn=False):
    value = check_existing_env_var(varname, default_value=default_value, flag_warn=flag_warn)
    path = check_and_convert_path(varname, value, check_write=check_write)
    return value, path


def what_package_am_i():
    path = os.path.dirname(os.path.abspath(__file__))
    cspp_x = path.split("/common")
    cspp_x_home = cspp_x[0]

    return cspp_x_home


def _ldd_verify(exe):
    '''
    check that a program is ready to run
    '''
    rc = call(['ldd', exe], stdout=os.tmpfile(), stderr=os.tmpfile())
    return rc == 0


#def check_env():
    #'''
    #Check that needed environment variables are set
    #'''
    #for key in EXTERNAL_BINARY.iterkeys():
        #if not _ldd_verify(EXTERNAL_BINARY[key]):
            #LOG.warning("{} executable is unlikely to run, is LD_LIBRARY_PATH set?".format(
                #EXTERNAL_BINARY[key]))


def env(**kv):
    '''
    augment environment with new values
    '''
    zult = dict(os.environ)
    zult.update(kv)

    return zult


def _convert_datetime(s):
    '''
    converter which takes strings from ASC and converts to computable datetime objects
    '''
    pt = s.rfind('.')
    micro_s = s[pt + 1:]
    micro_s += '0' * (6 - len(micro_s))
    #when = dt.datetime.strptime(s[:pt], '%Y-%m-%d %H:%M:%S').replace(microsecond = int(micro_s))
    when = datetime.strptime(s[:pt], '%Y-%m-%d %H:%M:%S').replace(microsecond=int(micro_s))
    return when


def _convert_isodatetime(s):
    '''
    converter which takes strings from ASC and converts to computable datetime objects
    '''
    pt = s.rfind('.')
    micro_s = s[pt + 1:]
    micro_s += '0' * (6 - len(micro_s))
    #when = dt.datetime.strptime(s[:pt], '%Y-%m-%d %H:%M:%S').replace(microsecond = int(micro_s))
    when = datetime.strptime(s[:pt], '%Y-%m-%dT%H:%M:%S').replace(microsecond=int(micro_s))
    return when


def make_time_stamp_d(timeObj):
    '''
    Returns a timestamp ending in deciseconds
    '''
    dateStamp = timeObj.strftime("%Y-%m-%d")
    #seconds = repr(int(round(timeObj.second + float(timeObj.microsecond) / 1000000.)))
    deciSeconds = int(round(float(timeObj.microsecond) / 100000.))
    deciSeconds = repr(0 if deciSeconds > 9 else deciSeconds)
    timeStamp = "{}.{}".format(timeObj.strftime("%H:%M:%S"), deciSeconds)
    return "{} {}".format(dateStamp, timeStamp)


def make_time_stamp_m(timeObj):
    '''
    Returns a timestamp ending in milliseconds
    '''
    dateStamp = timeObj.strftime("%Y-%m-%d")
    #seconds = repr(int(round(timeObj.second + float(timeObj.microsecond) / 1000000.)))
    milliseconds = int(round(float(timeObj.microsecond) / 1000.))
    milliseconds = repr(000 if milliseconds > 999 else milliseconds)
    timeStamp = "{}.{}".format(timeObj.strftime("%H:%M:%S"), str(milliseconds).zfill(3))
    return "{} {}".format(dateStamp, timeStamp)


def execution_time(startTime, endTime):
    '''
    Converts a time duration in seconds to days, hours, minutes etc...
    '''

    time_dict = {}

    delta = endTime - startTime
    days, remainder = divmod(delta, 86400.)
    hours, remainder = divmod(remainder, 3600.)
    minutes, seconds = divmod(remainder, 60.)

    time_dict['delta'] = delta
    time_dict['days'] = int(days)
    time_dict['hours'] = int(hours)
    time_dict['minutes'] = int(minutes)
    time_dict['seconds'] = seconds

    return time_dict


class NonBlockingStreamReader:
    '''
    Implements a reader for a data stream (associated with a subprocess) which
    does not block the process. This is done by writing the stream to a queue
    (decoupling the stream from the reading), and then slurping data off of the
    queue and passing it to wherever it's needed.
    '''

    def __init__(self, stream):
        '''
        stream: the stream to read from.
                Usually a process' stdout or stderr.
        '''

        self.stream = stream
        self.queue = Queue()

        def _populateQueue(stream, queue):
            '''
            Collect lines from 'stream' and put them in 'queue'.
            '''

            try:
                while True:
                    line = stream.readline()
                    if line:
                        queue.put(line)
                    else:
                        raise UnexpectedEndOfStream
                        pass
            except UnexpectedEndOfStream:
                LOG.debug("The process output stream has ended.")
            except ValueError:
                LOG.debug("ValueError: The process output stream has ended.")

        self.thread = Thread(target=_populateQueue, args=(self.stream, self.queue))
        self.thread.daemon = True
        self.thread.start()  # start collecting lines from the stream

    def readline(self, timeout=None):
        try:
            return self.queue.get(block=timeout is not None,
                                  timeout=timeout)
        except Empty:
            #print "Need to close the thread"
            return None


class UnexpectedEndOfStream(Exception):
    pass


def execute_binary_captured_inject_io(work_dir, cmd, err_dict, log_execution=True, log_stdout=True,
                                      log_stderr=True, **kv):
    '''
    Execute an external script, capturing stdout and stderr without blocking the
    called script.
    '''

    LOG.debug('executing {} with kv={}'.format(cmd, kv))
    pop = Popen(cmd,
                cwd=work_dir,
                env=env(**kv),
                shell=True,
                stdin=PIPE,
                stdout=PIPE,
                stderr=PIPE,
                close_fds=True)

    # wrap pop.std* streams with NonBlockingStreamReader objects:
    nbsr_stdout = NonBlockingStreamReader(pop.stdout)
    nbsr_stderr = NonBlockingStreamReader(pop.stderr)

    error_keys = err_dict['error_keys']
    del(err_dict['error_keys'])

    # get the output
    out_str = ""
    while pop.poll() is None and nbsr_stdout.thread.is_alive() and nbsr_stderr.thread.is_alive():

        '''
        Trawl through the stdout stream
        '''
        output_stdout = nbsr_stdout.readline(0.01)  # 0.01 secs to let the shell output the result

        if output_stdout is not None:

            # Gather the stdout stream for output to a log file.
            time_obj = datetime.utcnow()
            time_stamp = make_time_stamp_m(time_obj)
            out_str += "{} (INFO)  : {}".format(time_stamp, output_stdout)

            # Search stdout for exe error strings and pass them to the logger.
            for error_key in error_keys:
                error_pattern = err_dict[error_key]['pattern']
                if error_pattern in output_stdout:
                    output_stdout = string.replace(output_stdout, "\n", "")
                    err_dict[error_key]['count'] += 1

                    if err_dict[error_key]['count_only']:
                        if err_dict[error_key]['count'] < err_dict[error_key]['max_count']:
                            LOG.warn(string.replace(output_stdout, "\n", ""))
                        if err_dict[error_key]['count'] == err_dict[error_key]['max_count']:
                            LOG.warn(string.replace(output_stdout, "\n", ""))
                            LOG.warn(
                                'Maximum number of "{}" messages reached,' +
                                'further instances will be counted only'.format(error_key))
                    else:
                        LOG.warn(string.replace(output_stdout, "\n", ""))
                    break

        '''
        Trawl through the stderr stream
        '''
        output_stderr = nbsr_stderr.readline()  # 0.1 secs to let the shell output the result

        if output_stderr is not None:

            # Gather the stderr stream for output to a log file.
            time_obj = datetime.utcnow()
            time_stamp = make_time_stamp_m(time_obj)
            out_str += "{} (WARNING) : {}".format(time_stamp, output_stderr)

        '''
        Check to see if the stdout and stderr streams are ended
        '''
        if not nbsr_stdout.thread.is_alive():
            LOG.debug("stdout thread has ended for {}".format(cmd.split(" ")[-1]))
        if not nbsr_stderr.thread.is_alive():
            LOG.debug("stderr thread has ended for {}".format(cmd.split(" ")[-1]))

    # Flush the remaining content in the stdout and stderr streams
    while True:
        try:
            # 0.01 secs to let the shell output the result
            output_stdout = nbsr_stdout.readline(0.01)
            # 0.1 secs to let the shell output the result
            output_stderr = nbsr_stderr.readline()

            if output_stdout is not None or output_stderr is not None:

                if output_stdout is not None:
                    # Gather the stdout stream for output to a log file.
                    time_obj = datetime.utcnow()
                    time_stamp = make_time_stamp_m(time_obj)
                    out_str += "{} (INFO)  : {}".format(time_stamp, output_stdout)

                if output_stderr is not None:
                    # Gather the stderr stream for output to a log file.
                    time_obj = datetime.utcnow()
                    time_stamp = make_time_stamp_m(time_obj)
                    out_str += "{} (WARNING)  : {}".format(time_stamp, output_stderr)
            else:
                break

        except IOError:
            pass

    # Poll for the return code. A "None" value indicates that the process hasn’t terminated yet.
    # A negative value -N indicates that the child was terminated by signal N
    max_rc_poll_attempts = 20
    rc_poll_attempts = 0
    continue_polling = True
    while continue_polling:
        if rc_poll_attempts == max_rc_poll_attempts:
            LOG.warn(
                'Maximum number of attempts ({}) of obtaining return code for {} reached,' +
                'setting to zero.'.format(rc_poll_attempts, cmd.split(" ")[-1],))
            rc = 0
            break

        rc = pop.returncode
        LOG.debug("{} : pop.returncode = {}".format(cmd.split(" ")[-1], rc))
        if rc is not None:
            continue_polling = False

        rc_poll_attempts += 1
        time.sleep(0.5)

    LOG.debug("{}: rc = {}".format(cmd, rc))

    return rc, out_str


def simple_sh(cmd, log_execution=True, *args, **kwargs):
    '''
    like subprocess.check_call, but returning the pid the process was given
    '''
    if STRACE_ENABLED:
        strace = open('strace.log', 'at')
        print >> strace, "= " * 32
        print >> strace, repr(cmd)
        cmd = ['strace'] + list(cmd)
        pop = Popen(cmd, *args, stderr=strace, **kwargs)
    else:
        pop = Popen(cmd, *args, **kwargs)

    pid = pop.pid
    startTime = time.time()
    rc = pop.wait()

    endTime = time.time()
    delta = endTime - startTime
    LOG.debug('statistics for "%s"' % ' '.join(cmd))
    if log_execution:
        log_common.status_line('Execution Time: %f Sec Cmd "%s"' % (delta, ' '.join(cmd)))
        #LOG.debug('Execution Time: %f Sec Cmd "%s"' % (delta, ' '.join(cmd)))

    if rc != 0:
        exc = CalledProcessError(rc, cmd)
        exc.pid = pid
        raise exc

    return pid


def profiled_sh(cmd, log_execution=True, *args, **kwargs):
    '''
    like subprocess.check_call, but returning the pid the process was given and logging as
    INFO the final content of /proc/PID/stat
    '''
    pop = Popen(cmd, *args, **kwargs)
    pid = pop.pid
    fn = '/proc/%d/status' % pid
    LOG.debug('retrieving %s statistics to caller dictionary' % fn)
    proc_stats = '-- no /proc/PID/status data --'

    rc = 0
    startTime = time.time()
    while True:
        time.sleep(1.0)

        rc = pop.poll()
        if rc is not None:
            break

        try:
            proc = file(fn, 'rt')
            proc_stats = proc.read()
            proc.close()
            del proc
        except IOError:
            LOG.warning('unable to get stats from %s' % fn)

    endTime = time.time()
    delta = endTime - startTime
    LOG.debug('statistics for "%s"' % ' '.join(cmd))

    if log_execution:
        log_common.status_line('Execution Time:  "%f" Sec Cmd "%s"' % (delta, ' '.join(cmd)))
        #LOG.debug('Execution Time:  "%f" Sec Cmd "%s"' % (delta, ' '.join(cmd)))

    LOG.debug(proc_stats)

    if rc != 0:
        exc = CalledProcessError(rc, cmd)
        exc.pid = pid
        raise exc

    return pid


# default sh() is to profile on linux systems
if os.path.exists('/proc') and PROFILING_ENABLED:
    sh = profiled_sh
else:
    sh = simple_sh

#if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG) we don't want basicConfig anymore
#    log_common.configure_logging(level=logging.DEBUG, FILE="testlog.log")


def get_return_code(num_unpacking_problems, num_xml_files_to_process,
                    num_no_output_runs, noncritical_problem, environment_error):
    '''
    based on problems encountered, print final disposition message, return
    return code to be passed back to caller. Non-zero return code indicates a
    critical problem was encountered.
    '''
    # considered a noncritical problem if there were any runs that crashed,
    # produced no output, where Geo failed, where ADL logs indicated a problem,
    # or where output SDRs failed the imaginary quality check

    # critical problems: set non-zero return code and log error messages
    rc = 0
    if num_unpacking_problems > 0:
        rc |= 2
        LOG.error('Failed to unpack input data.')
    # skipping this check if no XML files to process
    if num_xml_files_to_process and (num_xml_files_to_process <= num_no_output_runs):
        rc |= 1
        LOG.error('Failed to generate any SDR granules.')
    if environment_error:
        rc |= 8
        LOG.error("Environment error.")

    # if critical error was encountered, print failure message and return error code
    if rc != 0:
        LOG.error('Failure. Refer to previous error messages')
        LOG.info('Failure. Refer to previous error messages')
        return rc

    # otherwise no errors or only non-critical errors: print success message and return 0
    if noncritical_problem:
        LOG.info('Normal Completion. Encountered some problems (refer to previous error messages).')
    else:
        LOG.info('Normal Completion.')
    return rc


def create_dir(dir):
    '''
    Create a directory
    '''
    returned_dir = copy(dir)
    LOG.debug("We want to create the dir {} ...".format(dir))

    try:
        if returned_dir is not None:
            returned_dir_path = os.path.dirname(returned_dir)
            returned_dir_base = os.path.basename(returned_dir)
            LOG.debug("returned_dir_path = {}".format(returned_dir_path))
            LOG.debug("returned_dir_base = {}".format(returned_dir_base))
            # Check if a directory and has write permissions...
            if not os.path.exists(returned_dir) and os.access(returned_dir_path, os.W_OK):
                LOG.debug("Creating directory {} ...".format(returned_dir))
                os.makedirs(returned_dir)
                # Check if the created dir has write permissions
                if not os.access(returned_dir, os.W_OK):
                    msg = "Created dir {} is not writable.".format(returned_dir)
                    raise CsppEnvironment(msg)
            elif os.path.exists(returned_dir):
                LOG.debug("Directory {} exists...".format(returned_dir))
                if not(os.path.isdir(returned_dir) and os.access(returned_dir, os.W_OK)):
                    msg = "Existing dir {} is not writable.".format(returned_dir)
                    raise CsppEnvironment(msg)
            else:
                raise CsppEnvironment("Cannot create {}".format(returned_dir))
    except CsppEnvironment:
        LOG.debug("Unable to create {}".format(returned_dir))
        LOG.debug(traceback.format_exc())
        returned_dir = None
    except OSError:
        LOG.debug("Unable to create new dir '{}' in {}".format(
            returned_dir_base, returned_dir_path))
        LOG.debug(traceback.format_exc())
        returned_dir = None
    except Exception:
        LOG.warning("General error for {}".format(returned_dir))
        LOG.debug(traceback.format_exc())
        returned_dir = None

    LOG.debug('Final returned_dir = {}'.format(returned_dir))
    return returned_dir


def setup_cache_dir(cache_dir, work_dir, cache_env_name):
    '''
    Setup the cache directory
    '''
    # Setting up cache dir
    returned_cache_dir = None

    # Explicit setting of cache dir from the command line option
    LOG.info('Checking cache directory...')
    if cache_dir is not None:
        cache_dir = os.path.abspath(os.path.expanduser(cache_dir))
        LOG.info('Creating cache dir from command line option ...')
        returned_cache_dir = copy(cache_dir)
        returned_cache_dir = create_dir(returned_cache_dir)

    # Explicit setting of cache dir failed, falling back to CSPP_ACTIVE_FIRE_CACHE_DIR...
    if returned_cache_dir is None:
        LOG.info('Creating cache dir from {}...'.format(cache_env_name))
        returned_cache_dir = check_existing_env_var(cache_env_name, default_value=None,
                                                    flag_warn=False)
        LOG.debug('{} = {}'.format(cache_env_name, returned_cache_dir))
        LOG.debug('returned_cache_dir = {}'.format(returned_cache_dir))
        current_dir = os.getcwd()
        returned_cache_dir = os.path.join(current_dir, cache_env_name.lower())
        returned_cache_dir = os.path.abspath(os.path.expanduser(returned_cache_dir))
        LOG.debug('returned_cache_dir = {}'.format(returned_cache_dir))
        returned_cache_dir = create_dir(returned_cache_dir)

    # Creating cache dir from env var has failed, try to create in the current dir.
    if returned_cache_dir is None:
        LOG.info('Creating cache dir in the the current dir...')
        current_dir = os.getcwd()
        returned_cache_dir = os.path.join(current_dir, cache_env_name.lower())
        returned_cache_dir = create_dir(returned_cache_dir)

    LOG.debug('Final returned_cache_dir = {}'.format(returned_cache_dir))
    return returned_cache_dir


def clean_cache(cache_dir, cache_time_window, granule_dt):
    """
    Purge the cache of old files.
    """

    # GRLWM_npp_d{}_t{}_e{}_b{}_ssec_dev.nc
    anc_file_pattern = ['(?P<kind>[A-Z]+)_',
                        '(?P<sat>[A-Za-z0-9]+)_', 'd(?P<date>\d+)_',
                        't(?P<start_time>\d+)_',
                        'e(?P<end_time>\d+)_',
                        'b(?P<orbit>\d+)_',
                        '(?P<site>[a-zA-Z0-9]+)_',
                        '(?P<domain>[a-zA-Z0-9]+)\.nc']
    anc_file_pattern = "".join(anc_file_pattern)
    re_anc_file_pattern = re.compile(anc_file_pattern)

    afire_anc_dirs = glob(os.path.join(cache_dir, '*_*_*_*-*h'))
    afire_anc_dirs.sort()
    #LOG.debug('afire_anc_dirs: {} :'.format(afire_anc_dirs))

    too_old_files = []
    future_files = []

    for afire_anc_dir in afire_anc_dirs:

        LOG.debug('afire_anc_dir: {}'.format(afire_anc_dir))

        afire_nc_files = glob(os.path.join(afire_anc_dir, 'GRLWM_*.nc'))
        afire_nc_files.sort()
        if afire_nc_files != []:
            LOG.debug('In {} :'.format(afire_anc_dir))
            LOG.debug(
                '\t{0}'.format(
                    ", ".join(["{}".format(os.path.basename(x)) for x in afire_nc_files])))

        afire_date_pattern = '%Y%m%d_%H%M%S'

        for files in afire_nc_files:
            file_basename = os.path.basename(files)

            file_info = dict(re_anc_file_pattern.match(file_basename).groupdict())
            #LOG.debug('{} : {}'.format(file_basename, file_info))

            date_str = '{}_{}'.format(file_info['date'], file_info['start_time'])
            deciseconds = int(date_str[-1])
            date_str = date_str[:-1]
            anc_file_dt = datetime.strptime(date_str, afire_date_pattern) + \
                timedelta(seconds=0.1 * deciseconds)
            LOG.debug('\tAncillary cache file has time = {0}'.format(anc_file_dt))

            time_diff = (granule_dt - anc_file_dt).total_seconds()
            time_diff_hours = time_diff / 3600.
            LOG.debug('\tTime between {0} and {1} is {2} seconds ({3} hours)'.format(
                granule_dt, anc_file_dt, time_diff, time_diff_hours))

            if time_diff < 0:
                LOG.debug(
                    '\tAncillary cache file {0} is in the future...'.format(
                        os.path.basename(files)))
                future_files.append(files)
            elif time_diff_hours > cache_time_window:
                LOG.debug(
                    '\tAncillary cache file {0} is more than {1} hours older than target...'.format(
                        os.path.basename(files), cache_time_window))
                too_old_files.append(files)
            else:
                pass

    if too_old_files != []:
        LOG.info("Removing old ancillary cache files from {0} ...".format(cache_dir))
        for too_old_file in too_old_files:
            LOG.debug('\tdeleting {0}'.format(os.path.basename(too_old_file)))
            if os.path.isfile(too_old_file):
                os.remove(too_old_file)
            else:
                LOG.warn("Removal candidate {0} does not exist".format(too_old_file))
    else:
        LOG.info("No old files need to be removed from the ancillary cache {0}".format(cache_dir))
