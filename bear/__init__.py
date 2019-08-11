"""
copyright: (c) 2018 by Kourosh Parsa.
"""
import os
import sys
from six import string_types
from multiprocessing import Process, Pipe
from threading import Thread
import time
from datetime import datetime
import subprocess
import hashlib
import cProfile
import pstats
import traceback
import logging
import psutil
import json
from bunch import Bunch, bunchify
from enum import Enum
import pickle
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
        res['error'] = u'{}\n{}'.format(ex, traceback_str)

    conn.send(res)
    conn.close()
    time.sleep(DELAY)  # to prevent deadlock due to not receiving the Pipe data
    if res['error'] is not None:
        sys.exit(1)


def profile(path):
    """ decorator for profiling """
    def decor(func):
        """ inner decorator """
        def wrap(*args, **kwargs):
            """ wraps your function """
            profiler = cProfile.Profile()
            profiler.enable()
            result = func(*args, **kwargs)
            profiler.disable()
            func_name = '?'
            if hasattr(func, '__qualname__'): # python3
                func_name = func.__qualname__
            else:  # python2
                func_name = func.func_name

            line = u'Profiling {} with arguments {} {}\n'\
                .format(func_name, args, kwargs)
            with open(path, 'a') as handle:
                handle.write(line)
            ps = pstats.Stats(profiler, stream=open(path, 'a')).sort_stats('cumulative')
            ps.print_stats()
            return result
        return wrap
    return decor


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
                obj = Bunch(timestamp=datetime.now(), used_mem=mem_obj.used, percent_mem_used=mem_obj.percent)
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
        duration = (self.task.end_time - self.task.start_time).seconds
        if duration == 1:
            duration = (self.task.end_time - self.task.start_time).microseconds / 1000000.0

        if isinstance(self.process, Process):
            res = self.task.parent_conn.recv()
            if res['error'] is not None or self.process.exitcode not in [0, None]:
                self.task.state = State.Failed
                logger.info('Task {} {} failed after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            else:
                self.task.state = State.Succeeded
                logger.info('Task {} {} succeeded after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            self.task.result = res['result']
            self.task.error = res['error']

        else: # instance of Popen
            self.task.result, self.task.error = self.process.communicate()
            if self.process.returncode != 0:
                self.task.state = State.Failed
                logger.info('Task {} {} failed after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            else:
                self.task.state = State.Succeeded
                logger.info('Task {} {} succeeded after {} seconds.'.format(self.task.id, self.task.func_name, duration))

    def run(self):
        """ execution code """
        while psutil.pid_exists(self.pid):
            try:
                if isinstance(self.process, Process):
                    if not self.process.is_alive():
                        break

                else: # instance of Popen
                    if not self.process.poll():
                        break

                mem = get_total_mem(self.pid)
                if mem > self.max_mem:
                    self.max_mem = mem
            except psutil.NoSuchProcess:
                break
            except Exception as ex:
                break

            time.sleep(DELAY)
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
    tasks = [Task(func) for param in params]
    for ind, param in enumerate(params):
        tasks[ind].start(*param, **kwargs)
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

        # set the task id and func_name:
        if isinstance(self.caller, string_types):
            name = self.caller.encode('utf8')
            self.id = hashlib.md5(name).hexdigest()
            self.func_name = self.caller

        elif self.caller is not None:
            if hasattr(self.caller, '__qualname__'):  # python3
                self.func_name = self.caller.__qualname__

            else:  # python2
                self.func_name = self.caller.func_name

            name = u'{},{},{}'.format(self.func_name, self.args, self.kwargs)
            name = name.encode('utf8')
            self.id = hashlib.md5(name).hexdigest()

    def start(self):
        if self.state == State.Succeeded:
            return # skip

        self.state = State.Started
        self.start_time = datetime.now()
        if isinstance(self.caller, string_types):
            self.process = subprocess.Popen(
                self.caller,
                stdout=self.stdout,
                stdin=self.stdin,
                stderr=self.stderr, shell=True)

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

    def join(self, *args):
        self.monitor.terminate()
        super(Task, self).join(*args)

    def wait(self):
        """ waits for the task to finish """
        if self.state == State.Created:
            logger.warn('You have not run the task yet.')

        if self.state == State.Failed:
            raise TaskError(self.id, self.func_name, self.args, self.kwargs, self.error)

        if self.state == State.Succeeded:
            return

        # otherwise state is 'Started'
        self.monitor.join() # end monitoring
        if self.state == State.Failed:
            raise TaskError(self.id, self.func_name, self.args, self.kwargs, self.error)

        return self.result

    def get_stats(self):
        """ returns a dict with stats about the task """
        self.wait()
        delta = self.end_time - self.start_time
        return {'id': self.id,
            'start': self.start_time.strftime("%H:%M:%S"),
            'end': self.end_time.strftime("%H:%M:%S"),
            'duration': delta.seconds,
            'max_mem': self.max_mem}


class Pipeline(object):
    """ orchestrates a pipeline """
    def __init__(self, resume=False, resume_path=None, memory_monitor_interval=None):
        """
        :param resume: boolean, default=False, set to True to be able to save the state and resume if some tasks fail
        :param resume_path: optional, where to save the pipeline state used to resume
        :param memory_monitor_interval: optional, if set, the pipeline monitors system memory on this interval in seconds
        """
        self.group_count = 0
        self.tasks = []
        self.resume = resume
        self.resume_path = os.path.join(os.path.expanduser("~"), '.bear')
        if resume_path:
            self.resume_path = resume_path

        self.system_monitor = None
        if memory_monitor_interval is not None:
            if memory_monitor_interval < 1:
                raise Exception('memory_monitor_interval cannot be less than 1')
            self.system_monitor = SystemMonitor(memory_monitor_interval)
            self.system_monitor.start()

    def terminate(self):
        self.system_monitor.terminate()

    def __create_tasks(self, func, arg_list, kwargs):
        """
        :param func: function signature
        :param arg_list: list of lists
        :param kwargs: dictionary
        :return: list of Task objects
        """
        new_tasks = []
        for args in arg_list:
            task = Task(func, args, kwargs, group_id=self.group_count)
            self.tasks.append(task)
            new_tasks.append(task)

        self.group_count += 1
        return new_tasks

    def __has_bandwidth(self, tasks, concurrency):
        active = sum(task.state == State.Started for task in tasks)
        return active < concurrency

    def __start_tasks(self, tasks, concurrency=1000):
        """
        :param tasks: list of Task objects
        :param concurrency: int
        starts the tasks without waiting for them to finish
        """
        while sum(task.state == State.Created for task in tasks):
            for task in tasks:
                if task.state == State.Created and self.__has_bandwidth(tasks, concurrency):
                    task.start()

            time.sleep(DELAY)  # necessary to avoid deadlock

    def __skip_previous_tasks(self, tasks):
        """
        :param tasks: list of Task
        if resume option is used, it updates the tasks to skip previously successful tasks
        """
        if self.resume and os.path.exists(self.resume_path):
            try:
                data = bunchify(pickle.load(open(self.resume_path, 'rb')))
                for ind, task in enumerate(tasks):
                    if task.id in data and data[task.id].state == State.Succeeded:
                        logger.info('Skipping task {}: {}'.format(task.func_name, task.id))
                        for attr in TASK_CLONED_ATTRS:
                            setattr(tasks[ind], attr, data[task.id][attr])

            except Exception as ex:
                logger.error('Found the pickle file but could not load it: %s', ex)

    def __save_tasks(self):
        if self.resume:
            tasks = {}
            for task in self.tasks:
                data = {}
                for attr in TASK_CLONED_ATTRS:
                    data[attr] = getattr(task, attr)
                tasks[task.id] = data

            pickle.dump(tasks, open(self.resume_path, "wb"))

    def sync(self, func, args, kwargs={}, concurrency=1000):
        """
        :param func: function signature
        :param args: list
        :param concurrency: int
        :param kwargs: dictionary
        :return: list of results
        runs tasks in parallel and waits for them to finish
        """
        tasks = self.__create_tasks(func, args, kwargs)
        self.__skip_previous_tasks(tasks)
        self.__start_tasks(tasks, concurrency=concurrency)
        for task in tasks:
            task.wait()

        self.__save_tasks()
        return [task.result for task in tasks]

    def async(self, func, args, kwargs={}):
        """
        :param func: function signature
        :param args: list
        :param kwargs: dictionary
        :return: list of Task objects
        runs tasks in parallel but does not wait for them to  finish
        """
        tasks = self.__create_tasks(func, args, kwargs)
        self.__start_tasks(tasks)
        return tasks

    def get_stats(self):
        """ returns a list of dict with task stats """
        return [task.get_stats() for task in self.tasks]

    def wait(self):
        """ waits for all the tasks to finish """
        for task in self.tasks:
            task.wait()

    def get_all_results(self):
        """ returns all results as a list """
        self.wait()
        return [task.result for task in self.tasks]

    def save_stats(self, path):
        json.dump(self.get_stats(), open(path, 'w'))

    def plot_tasks_duration(self, path):
        """
        :param path: absolute path of the image to save to
        plots the task durations and saves it to an image
        """
        plotting.plot_tasks_duration(self.tasks, path)

    def plot_tasks_memory(self, path):
        """
        :param path: absolute path of the image to save to
        plots the task max memory and saves it to an image
        """
        plotting.plot_tasks_memory(self.tasks, path)

    def plot_system_memory(self, path):
        """
        :param path: absolute path of the image to save to
        plots the task memories and system memory. It saves the image to a file
        """
        self.system_monitor.stop_recording = True
        sys_mem = self.system_monitor.data
        self.system_monitor.stop_recording = False

        if len(sys_mem) < 1:
            raise Exception('There is no system memory usage data. '\
                'You need to turn it on when creating a Pipeline.')
        plotting.plot_system_memory(self.tasks, path, sys_mem)
