#!/usr/bin/env python
# encoding: utf-8
"""
Created Oct 2011 by R.K.Garcia <rayg@ssec.wisc.edu>
Copyright (c) 2011 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
import string
import sys
import logging
import log_common
import time
import types
import fileinput
from subprocess import Popen, CalledProcessError, call, PIPE

from datetime import datetime


LOG = logging.getLogger(__name__)

PROFILING_ENABLED = os.environ.get('CSPP_PROFILE', None) is not None
STRACE_ENABLED = os.environ.get('CSPP_STRACE', None) is not None


class SingleLevelFilter(logging.Filter):
    """
     ref: http://stackoverflow.com/questions/1383254/logging-streamhandler-and-standard-streams
    """
    def __init__(self, passlevels, reject):
        """


            :rtype : object
            :param passlevels:
            :param reject:
            """
        super(SingleLevelFilter, self).__init__()
        self.passlevels = set(passlevels)
        self.reject = reject

    def filter(self, record):
        """

        :param record:
        :return:
        """
        if self.reject:
            return record.levelno not in self.passlevels
        else:
            return record.levelno in self.passlevels


def split_search_path(s):
    """break a colon-separated list of directories into a list of directories, else empty-list"""
    if not s:
        return []

    back_list = []
    for path in s.split(':'):
        back_list.append(os.path.abspath(path))

    return back_list


def _replaceAll(intputfile, searchExp, replaceExp):
    """

    :param intputfile:
    :param searchExp:
    :param replaceExp:
    """
    for line in fileinput.input(intputfile, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        sys.stdout.write(line)
    fileinput.close()

#    ("RangeDateTime" DATETIMERANGE EQ "2014-01-13 11:22:39.900000" "2014-01-13 11:22:59.900000")


class AscLineParser(object):
    def time_range(self, ascLine):
        """

        :param ascLine:
        :return:
        """
        day, time = self.extract_time_range_tokens(ascLine)
        return self.time_from_tokens(day, time)

    def extract_time_range_tokens(self, ascLine):
        return ascLine.split('"')[3:4][0].split(' ')

    def time_from_tokens(self, day, time):
        dt = datetime.strptime(day + time, '%Y-%m-%d%H:%M:%S.%f')
        return dt


def link_files(dest_path, files):
    """
    Link ancillary files into a destination directory.
    :rtype : None
    """
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


def _testParser():
    """


    """
    dt = AscLineParser().time_range(
        '("RangeDateTime" DATETIMERANGE EQ "2014-01-13 11:22:39.900000" "2014-01-13 11:22:59.900000")')
    print dt

class CsppEnvironment(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def check_and_convert_path(key, a_path, check_write=False):
    """
    Make sure the path or paths specified exist
    Return the path or list of absolute paths that exist
    """
    abs_locations = []
    if ":" in a_path:
        paths = a_path.split(":")
    elif isinstance(a_path, types.StringTypes):
        paths = [a_path]
    else:
        paths = a_path

    for path in paths:
        if not os.path.exists(path):
            if key :
                msg="Environment variable %s refers to a path that does not exists.  %s=%s" % (key, key, path)
            else :
                msg="Required path %s does not exist.  " % (path)

            raise CsppEnvironment(msg)
            sys.exit(2)
        else:
            LOG.debug("Found: %s at %s %s" % (key, path, os.path.abspath(path)))
            abs_locations.append(os.path.abspath(path))

        if check_write:
            if not os.access(path, os.W_OK):
                msg="Path exists but is not writable %s=%s" % (key, path)
                raise CsppEnvironment(msg)

    # return a string if only one and an array if more
    if len(abs_locations) == 1:
        return abs_locations[0]
    else:
        #return abs_locations
        # return a :-joined string for use in an env variable
        return ':'.join(abs_locations)


def check_existing_env_var(varname, default_value=None):
    """
    Check for vaiable if it exists use vale otherwise use default
    """

    if varname in os.environ:
        value = os.environ.get(varname)
    else:
        if default_value is not None:
            value = default_value
        else:
            print >> sys.stderr, "ERROR: %s is not set, please update environment and re-try" % varname
            LOG.error("Environment variable missing. %s" % varname)
            sys.exit(9)

    return value


def check_and_convert_env_var(varname, check_write=False, default_value=None):
    value = check_existing_env_var(varname, default_value=default_value)
    path = check_and_convert_path(varname, value, check_write=check_write)
    return path


def what_package_am_i():
    path = os.path.dirname(os.path.abspath(__file__))
    cspp_x = path.split("/common")
    cspp_x_home = cspp_x[0]

    return cspp_x_home

def _ldd_verify(exe):
    """check that a program is ready to run"""
    rc = call(['ldd', exe], stdout=os.tmpfile(), stderr=os.tmpfile())
    return rc == 0


def check_env():
    """ Check that needed environment variables are set"""

    for key in EXTERNAL_BINARY.iterkeys():
        if not _ldd_verify(EXTERNAL_BINARY[key]):
            LOG.warning("%r executable is unlikely to run, is LD_LIBRARY_PATH set?" % EXTERNAL_BINARY[key])


def env(**kv):
    """augment environment with new values"""
    zult = dict(os.environ)
    zult.update(kv)

    return zult


def make_time_stamp_d(timeObj):
    """
    Returns a timestamp ending in deciseconds
    """
    dateStamp = timeObj.strftime("%Y-%m-%d")
    seconds = repr(int(round(timeObj.second + float(timeObj.microsecond)/1000000.)))
    deciSeconds = int(round(float(timeObj.microsecond)/100000.))
    deciSeconds = repr(0 if deciSeconds > 9 else deciSeconds)
    timeStamp = "{}.{}".format(timeObj.strftime("%H:%M:%S"),deciSeconds)
    return "{} {}".format(dateStamp,timeStamp)


def make_time_stamp_m(timeObj):
    """
    Returns a timestamp ending in milliseconds
    """
    dateStamp = timeObj.strftime("%Y-%m-%d")
    seconds = repr(int(round(timeObj.second + float(timeObj.microsecond)/1000000.)))
    milliseconds = int(round(float(timeObj.microsecond)/1000.))
    milliseconds = repr(000 if milliseconds > 999 else milliseconds)
    timeStamp = "{}.{}".format(timeObj.strftime("%H:%M:%S"),str(milliseconds).zfill(3))
    return "{} {}".format(dateStamp,timeStamp)


import threading
from threading import Thread, Event
from Queue import Queue, Empty

class NonBlockingStreamReader:
    """
    Implements a reader for a data stream (associated with a subprocess) which
    does not block the process. This is done by writing the stream to a queue
    (decoupling the stream from the reading), and then slurping data off of the
    queue and passing it to wherever it's needed.
    """

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

        self.thread = Thread(target = _populateQueue, args = (self.stream, self.queue))
        self.thread.daemon = True
        self.thread.start() #start collecting lines from the stream

    def readline(self, timeout = None):
        try:
            return self.queue.get(block = timeout is not None,
                    timeout = timeout)
        except Empty:
            #print "Need to close the thread"
            return None

class UnexpectedEndOfStream(Exception):
    pass


def execute_binary_captured_inject_io(work_dir, cmd, log_execution=True, log_stdout=True,log_stderr=True, **kv):
    """
    Execute an external script, capturing stdout and stderr without blocking the
    called script.
    """

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

    error_strings = ['FAILURE','failure','FAILED','failed','FAIL','fail',
            'error','ERROR','ABORT','abort','ABORTING','aborting']
    temporal_warning_string = "Temporal_Data"
    temporal_warning = False
    hdf_too_big_string = "Cannot create HDF writing for SDS"
    hdf_too_big_warning = False

    # get the output
    out_str = ""
    while pop.poll()==None and nbsr_stdout.thread.is_alive() and nbsr_stderr.thread.is_alive():
        output_stdout = nbsr_stdout.readline(0.01) # 0.01 secs to let the shell output the result
        if output_stdout is not None:
            time_obj = datetime.utcnow()
            time_stamp = make_time_stamp_m(time_obj)
            out_str += "{} (INFO)  : {}".format(time_stamp,output_stdout)

            # Search stdout for geocat error strings and pass them to the logger.
            for err_str in error_strings:
                if err_str in output_stdout:
                    if temporal_warning_string in output_stdout:
                        temporal_warning = True
                    elif hdf_too_big_string in output_stdout:
                        hdf_too_big_warning = True
                    else:
                        LOG.warn(string.replace(output_stdout,"\n",""))

        output_stderr = nbsr_stderr.readline() # 0.1 secs to let the shell output the result

        if output_stderr is not None:
            time_obj = datetime.utcnow()
            time_stamp = make_time_stamp_m(time_obj)
            stderr_str = "{}".format(output_stderr)
            LOG.error(string.replace(stderr_str,"\n",""))
            out_str += "{} (ERROR) : {}".format(time_stamp,stderr_str)

        if not nbsr_stdout.thread.is_alive():
            LOG.debug("stdout thread has ended for segment {} of {}".format(kv['segment'],cmd.split(" ")[-1]))
        if not nbsr_stderr.thread.is_alive():
            LOG.debug("stderr thread has ended for segment {} of {}".format(kv['segment'],cmd.split(" ")[-1]))

    # FIXME: Sometimes the nbsr_stdout and nbsr_stderr threads haven't finished
    #        yet.
    try:
        anc_stdout, anc_stderr = pop.communicate()
    except IOError:
        pass

    if temporal_warning == True :
        LOG.warn("Previous timesteps were not available to satisfy temporal processing requirements")

    if hdf_too_big_warning == True :
        LOG.error("Geocat tried to create a HDF4 file larger than 2GB. Try breaking up the processing into segments using '--line_segments 2 --element_segments 2'")
        rc = 1
        return rc, out_str

    # Poll for the return code. A "None" value indicates that the process hasn’t terminated yet.
    # A negative value -N indicates that the child was terminated by signal N
    #rc = pop.returncode
    max_rc_poll_attempts = 10
    rc_poll_attempts = 0
    continue_polling = True
    while continue_polling:
        if rc_poll_attempts == max_rc_poll_attempts:
            LOG.warn(
            'Maximum number of attempts ({}) of obtaining geocat return code for segment {} of {} reached, setting to zero.'
            .format(rc_poll_attempts,kv['segment'],cmd.split(" ")[-1],))
            rc = 0
            break

        rc = pop.returncode
        LOG.debug("Segment {} of {} : pop.returncode = {}".format(kv['segment'],cmd.split(" ")[-1],rc))
        if rc != None:
            continue_polling = False

        rc_poll_attempts += 1
        time.sleep(0.5)


    LOG.debug("Segment {} of {}: rc = {}".format(kv['segment'],cmd.split(" ")[-1],rc))

    return rc, out_str


def execute_binary_captured_io(work_dir, cmd, log_execution=True, log_stdout=True,log_stderr=True, **kv):
    """
    Execute the specifed ancillary script.
    process the ouptut and return the file names.
    """
    startTime = time.time()

    LOG.debug('executing %r with kv=%r' % (cmd, kv))
    pop = Popen(cmd,
                cwd=work_dir,
                env=env(**kv),
                shell=True,
                stdin=PIPE,
                stdout=PIPE,
                stderr=PIPE,
                close_fds=True)

    anc_stdout, anc_stderr = pop.communicate()
    rc = pop.returncode

    endTime = time.time()
    delta = endTime - startTime

    LOG.debug('statistics for "%s"' % cmd.split('v ')[0])
    if log_execution:
        log_common.status_line('Execution Time: %f Sec Cmd "%s"' % (delta, cmd))
        #LOG.debug('Execution Time: %f Sec Cmd "%s"' % (delta, cmd))

    if rc == 0:
        LOG.debug("success " + cmd)

        if log_stdout :
            LOG.info(anc_stdout.strip())
            LOG.info(anc_stderr.strip())

    elif rc == 1:
        if log_stderr :
            LOG.debug(anc_stdout)
            LOG.error("stderr:" + anc_stderr)

    else:
        LOG.warn("what " + cmd)

    if rc != 0:
        if log_stdout and log_stderr :
            LOG.debug(anc_stdout)
            LOG.error(anc_stderr)
            LOG.info('rc %d'%rc)

    return rc, anc_stdout, anc_stderr


def simple_sh(cmd, log_execution=True, *args, **kwargs):
    """like subprocess.check_call, but returning the pid the process was given"""
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
    """
    like subprocess.check_call, but returning the pid the process was given and logging as
    INFO the final content of /proc/PID/stat

    :param cmd:
    :param log_execution:
    :param args:
    :param kwargs:
    """
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
