"""
copyright: (c) 2018 by Kourosh Parsa.
"""
import os
import sys
from six import string_types
from multiprocessing import Process, Pipe
from threading import Thread
import time
import uuid
from datetime import datetime
import subprocess
import traceback
import logging
import psutil
import json
from enum import Enum
from bear import plotting

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
TASK_CLONED_ATTRS = ['timeout', 'reserved_mem', 'args', 'kwargs', 'id',
                     'start_time', 'end_time', 'max_mem', 'state', 'error', 'result', 'func_name']
DELAY = 0.1


class State(Enum):
    Created = 0
    Started = 1
    Succeeded = 2
    Failed = 3


def callit(func, conn, *args, **kwargs):
    """ executes a function and sends the results via a pipe """
    res = {'error': None, 'result': None}
    try:
        res['result'] = func(*args, **kwargs)
    except Exception as ex:
        traceback_str = traceback.format_exc()
        logger.error(traceback_str)
        res['error'] = u'{}\n{}'.format(ex, traceback_str)

    try:
        conn.send(res)
    except BrokenPipeError:
        pass

    conn.close()
    time.sleep(DELAY)  # to prevent deadlock due to not receiving the Pipe data

    if res['error'] is not None:
        sys.exit(1)


def get_total_mem(pid):
    """
    pid: int, process id
    returns the total RSS memory take from RAM used by the process and
    all it's children
    """
    proc = psutil.Process(pid)
    mem = proc.memory_info().rss
    for child in proc.children(recursive=True):
        mem += get_total_mem(child.pid)
    return mem


class Mem(object):
    def __init__(self, timestamp, used_mem, percent_mem_used):
        self.timestamp = timestamp
        self.used_mem = used_mem
        self.percent_mem_used = percent_mem_used


class SystemMonitor(Thread):
    """ Thread used for monitoring system resources """

    def __init__(self, interval):
        Thread.__init__(self)
        self.daemon = True
        self.interval = interval
        self.data = []
        self.stop_recording = False

    def run(self):
        while self.interval >= 1:
            if not self.stop_recording:
                mem_obj = psutil.virtual_memory()
                obj = Mem(datetime.now(), mem_obj.used, mem_obj.percent)
                self.data.append(obj)
            time.sleep(self.interval)


class TaskMonitor(Thread):
    """ Thread used for monitoring the RSS memory and state of a process """

    def __init__(self, task):
        """ constructor """
        Thread.__init__(self)
        self.max_mem = 0
        self.task = task
        self.process = task.process
        self.pid = task.process.pid
        self.daemon = True

    def _set_task_attrs(self):
        """ precondition:
        if process is a Process object, is_alive() must be False
        else (process is a Popen object), poll() must be False
        """
        self.task.end_time = datetime.now()
        self.task.max_mem = self.max_mem
        duration = (self.task.end_time - self.task.start_time).total_seconds()
        if duration == 1:
            duration = (self.task.end_time - self.task.start_time).microseconds / 1000000.0

        if isinstance(self.process, Process):
            res = self.task.parent_conn.recv()
            if res['error'] is not None or self.process.exitcode not in [0, None]:
                self.task.state = State.Failed
                logger.info('Task {} {} failed after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            else:
                self.task.state = State.Succeeded
                logger.info(
                    'Task {} {} succeeded after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            self.task.result = res['result']

            self.task.error = res['error']

        else:  # instance of Popen
            self.task.result, self.task.error = self.process.communicate()
            if self.process.returncode != 0:
                self.task.state = State.Failed
                logger.info('Task {} {} failed after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            else:
                self.task.state = State.Succeeded
                logger.info(
                    'Task {} {} succeeded after {} seconds.'.format(self.task.id, self.task.func_name, duration))

    def run(self):
        """ execution code """
        while psutil.pid_exists(self.pid):
            try:
                if isinstance(self.process, Process):
                    if not self.process.is_alive():
                        break

                else:  # instance of Popen
                    if not self.process.poll():
                        break

                mem = get_total_mem(self.pid)
                if mem > self.max_mem:
                    self.max_mem = mem
            except psutil.NoSuchProcess:
                break
            except Exception as ex:
                break

            #time.sleep(DELAY)

        self._set_task_attrs()


def _get_sub_params(params, concurrency):
    """ @params: list
    @concurrency: int
    returns a list of lists of sub parameters
    """
    if concurrency is None or concurrency < 1 or concurrency >= len(params):
        return [params]

    start = 0
    end = concurrency
    res = []
    while start <= len(params):
        res.append(params[start:end])
        start += concurrency
        end = min(end + concurrency, len(params))

    return res


def parallel(func, params, **kwargs):
    """ runs a function with a set of parameters in parallel
    but does not wait for them to finish
    func: a function reference
    params: list of argument lists
    """
    if 'kwargs' in kwargs:
        assert isinstance(kwargs['kwargs'], list), 'kwargs Must be a list'
        assert len(kwargs['kwargs']) == len(params), 'The length of params and kwargs must match'
    tasks = []
    for ind, param in enumerate(params):
        task = Task(func)
        if 'kwargs' in kwargs:
            task.start(args=param, kwargs=kwargs['kwargs'][ind])
        else:
            task.start(args=param, kwargs=kwargs)
        tasks.append(task)
    return tasks


def wait_for(tasks):
    """ waits for tasks to finish
    tasks: list of Task objects
    """
    fail_count = 0
    for task in tasks:
        try:
            task.wait()
        except TaskError as err:
            fail_count += 1
            logger.error(err)
    if fail_count > 0:
        raise TaskError('', tasks[0].func_name, '', '', '{} tasks failed.'.format(fail_count))


class TaskError(Exception):
    """ Exception thrown when a Task fails and you call the wait command
    """

    def __init__(self, task_id, func_name, args, kwargs, error):
        super(TaskError, self).__init__(u'Task {} failed: {} {} {}\nError:\n{}'
                                        .format(task_id, func_name, args, kwargs, error))


class Task(object):
    """ To execute a task """

    def __init__(self, caller, args=[], kwargs={}, timeout=None,
                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                 stdin=subprocess.PIPE, group_id=None):
        """
        caller: a function to run or a bash command in string
        args: list
        kwargs: dict
        timeout: in seconds
        reserved_mem is in bytes. It's how much memory this task requires
            when using the Pipeline, it allows you to wait until there is enough
            memory in the operating system for the task to run successfully
        group_id: an integer to group tasks that were run with a single sync together
        """
        self.timeout = timeout
        self.reserved_mem = None
        self.caller = caller
        self.args = args
        self.kwargs = kwargs
        self.process = None
        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin
        self.id = None
        self.stdout = stdout
        self.start_time = None
        self.end_time = None
        self.max_mem = 0
        self.state = State.Created
        self.parent_conn = None
        self.child_conn = None
        self.error = None
        self.result = None
        self.func_name = None
        self.monitor = None
        self.group_id = group_id
        self.id = str(uuid.uuid4())

        # set the task id and func_name:
        if isinstance(self.caller, string_types):
            name = self.caller.encode('utf8')
            self.func_name = name

        elif self.caller is not None:
            if hasattr(self.caller, '__qualname__'):  # python3
                self.func_name = self.caller.__qualname__

            else:  # python2
                self.func_name = self.caller.func_name

    def get_duration(self):
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time).total_seconds()

    def get_memory(self):
        return self.max_mem

    def start(self, args=None, kwargs=None):
        if args:
            self.args = args

        if kwargs:
            self.kwargs = kwargs

        if self.state == State.Succeeded:
            return  # skip

        self.state = State.Started
        self.start_time = datetime.now()
        if isinstance(self.caller, string_types):
            self.process = subprocess.Popen(
                self.caller,
                stdout=self.stdout,
                stdin=self.stdin,
                stderr=self.stderr,
                shell=True,
                start_new_session=False)

        elif self.caller is not None:
            self.parent_conn, self.child_conn = Pipe()
            xargs = [self.caller, self.child_conn] + list(self.args)
            self.process = Process(target=callit, args=xargs, kwargs=self.kwargs)
            self.process.start()

        if self.process is not None:
            line = 'Task {} PID: {} is running {}{}, keywords:{}'
            line = line.format(self.id, self.process.pid,
                               self.func_name, self.args, self.kwargs)
            logger.info(line)
            self.monitor = TaskMonitor(self)
            self.monitor.start()

    def wait(self):
        """ waits for the task to finish """
        if self.state == State.Created:
            logger.warn('You have not run the task yet.')

        if self.state == State.Failed:
            raise TaskError(self.id, self.func_name, self.args, self.kwargs, self.error)

        if self.state == State.Succeeded:
            return self.result

        # otherwise state is 'Started'

        self.result = self.monitor.task.result
        if self.state == State.Failed:
            raise TaskError(self.id, self.func_name, self.args, self.kwargs, self.error)

        self.monitor.join()  # end monitoring
        return self.result

    def get_result(self):
        """ waits for the task to end and returns the result """
        return self.wait()

    def get_stats(self):
        """ returns a dict with stats about the task """
        self.wait()
        delta = self.end_time - self.start_time
        return {'id': self.id,
                'start': self.start_time.strftime("%H:%M:%S"),
                'end': self.end_time.strftime("%H:%M:%S"),
                'duration': delta.total_seconds(),
                'max_mem': self.max_mem,
                'func_name': self.func_name,
                'group_id': self.group_id}


