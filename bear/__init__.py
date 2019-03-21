"""
copyright: (c) 2018 by Kourosh Parsa.
"""
import sys
from six import string_types
from multiprocessing import Process, Pipe
import time
from datetime import datetime
from threading import Thread
import subprocess
import hashlib
import cProfile
import pstats
import traceback
import logging
import psutil
import json
from bear import plotting
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
            else: # python2
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
        if isinstance(self.process, Process):
            res = self.task.parent_conn.recv()
            if res['error'] is not None or self.process.exitcode not in [0, None]:
                self.task.state = 'Failed'
                logger.info('Task {} {} failed after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            else:
                self.task.state = 'Succeeded'
                logger.info('Task {} {} succeeded after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            self.task.result = res['result']
            self.task.error = res['error']

        else: # instace of Popen
            self.task.result, self.task.error = self.process.communicate()
            if self.process.returncode != 0:
                self.task.state = 'Failed'
                logger.info('Task {} {} failed after {} seconds.'.format(self.task.id, self.task.func_name, duration))

            else:
                self.task.state = 'Succeeded'
                logger.info('Task {} {} succeeded after {} seconds.'.format(self.task.id, self.task.func_name, duration))


    def run(self):
        """ execution code """
        while psutil.pid_exists(self.pid):
            try:
                if isinstance(self.process, Process):
                    if not self.process.is_alive():
                        break

                else: # instace of Popen
                    if not self.process.poll():
                        break

                mem = get_total_mem(self.pid)
                if mem > self.max_mem:
                    self.max_mem = mem
            except psutil.NoSuchProcess:
                break
            except Exception as ex:
                break

            time.sleep(0.2)
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


def parallel_wait(func, params, concurrency=None, **kwargs):
    """
    runs tasks in parallel and waits for them to finish
    """
    tasks = []
    for sub_params in _get_sub_params(params, concurrency): 
        partial_tasks = parallel(func, sub_params, **kwargs)
        wait_for(partial_tasks)
        tasks = tasks + partial_tasks
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
        super(TaskError, self).__init__(u'Task {} failed: {} {} {}\nError:\n{}'\
            .format(task_id, func_name, args, kwargs, error))


class Task(object):
    """ To execute a task """
    def __init__(self, caller, timeout=None, reserved_mem=None,\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
        stdin=subprocess.PIPE):
        """
        caller: a function to run or a bash command in string
        timeout: in seconds
        reserved_mem is in bytes. It's how much memory this task requires
            when using the Pipeline, it allows you to wait until there is enough
            memory in the operating system for the task to run successfuly
        """
        self.timeout = timeout
        self.reserved_mem = None
        self.caller = caller
        self.process = None
        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin
        self.id = None
        self.stdout = stdout
        self.args = None
        self.kwargs = None
        self.start_time = None
        self.end_time = None
        self.max_mem = 0
        self.state = 'Created'
        self.parent_conn = None
        self.child_conn = None
        self.error = None
        self.result = None
        self.func_name = None

    def start(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.start_time = datetime.now()
        self.state = 'Started'
        func_name = None
        if isinstance(self.caller, string_types):
            self.process = subprocess.Popen(self.caller,\
                stdout=self.stdout,\
                stdin=self.stdin,\
                stderr=self.stderr, shell=True)
            name = self.caller.encode('utf8')
            self.id = hashlib.md5(name).hexdigest()
            self.func_name = self.caller

        elif self.caller is not None:
            if hasattr(self.caller, '__qualname__'): # python3
                self.func_name = self.caller.__qualname__
            else: # python2
                self.func_name = self.caller.func_name

            name = u'{},{},{}'.format(self.func_name, args, kwargs)
            name = name.encode('utf8')
            self.id = hashlib.md5(name).hexdigest()
            self.parent_conn, self.child_conn = Pipe()
            xargs = [self.caller, self.child_conn] + list(args)
            self.process = Process(target=callit, args=xargs, kwargs=kwargs)
            self.process.start()

        if self.process is not None:
            line = 'Task {} PID: {} is running {}{}, keywords:{}'
            line = line.format(self.id, self.process.pid,\
                self.func_name, args, kwargs)
            logger.info(line)
            self.monitor = TaskMonitor(self)
            self.monitor.start()

    def wait(self):
        """ waits for the task to finish """
        if self.state == 'Created':
            logger.warn('You have not run the task yet.')

        if self.state == 'Failed':
            raise TaskError(self.id, self.func_name, self.args, self.kwargs, self.error)

        if self.state == 'Succeeded':
            return

        # otherwise state is 'Started'
        self.monitor.join() # end monitoring
        if self.state == 'Failed':
            raise TaskError(self.id, self.func_name, self.args, self.kwargs, self.error)

        return self.result


    def get_stats(self):
        """ returns a dict with stats about the task """
        self.wait()
        delta = self.end_time - self.start_time
        return {'id': self.id,\
            'start': self.start_time.strftime("%H:%M:%S"),\
            'end': self.end_time.strftime("%H:%M:%S"),\
            'duration': delta.seconds,\
            'max_mem': self.max_mem}


class Pipeline(object):
    """ orchestrates a pipeline """
    def __init__(self):
        self.tasks = []

    def sync(self, func, params, concurrency=None, **kwargs):
        """ runs tasks in parallel and waits for them to finish """
        tasks = parallel_wait(func, params, concurrency, **kwargs)
        for task in tasks:
            self.tasks.append(task)

    def async(self, func, params, **kwargs):
        """ runs tasks in parallel but does not
        wait for them to  finish """
        for task in parallel(func, params, **kwargs):
            self.tasks.append(task)

    def get_stats(self):
        """ returns a list of dict with task stats """
        return [task.get_stats() for task in self.tasks]

    def wait(self):
        """ waits for all the tasks to finish """
        for task in self.tasks:
            task.wait()

    def results(self):
        """ returns all results as a list """
        self.wait()
        return [task.result for task in self.tasks]

    def save_stats(self, path):
        json.dump(self.get_stats(), open(path, 'w'))

    def plot_tasks_duration(self, path):
        """ plots the task durations and saves it to an image """
        plotting.plot_tasks_duration(self.tasks, path)

    def plot_tasks_memory(self, path):
        """ plots the task max memory and saves it to an image """
        plotting.plot_tasks_memory(self.tasks, path)

