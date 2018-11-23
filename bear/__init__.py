"""
copyright: (c) 2018 by Kourosh Parsa.
"""
from multiprocessing import Process, Pipe
import time
from datetime import datetime
import psutil
from threading import Thread
import subprocess
import hashlib
import cProfile
import pstats
import traceback
import logging
logger = logging.getLogger(__name__)

def callit(func, conn, *args):
    res = {'error': None, 'result': None}
    try:
        res['result'] = func(*args)
    except Exception as ex:
        tb = traceback.format_exc()
        res['error'] = u'{}\n{}'.format(ex, tb)

    conn.send(res)
    conn.close()


class MyProcess(Process):
    def __init__(self, *args, **kwargs):
        mp.Process.__init__(self, *args, **kwargs)
        self.parent_conn, self.child_conn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self.child_conn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self.child_conn.send((e, tb))

def profile(path):
    def decor(func):
        def wrap(*args, **kwargs):
            pr = cProfile.Profile()
            pr.enable()
            result = func(*args, **kwargs)
            pr.disable()
            open(path, 'a').write('Profiling {} with arguments {} {}\n'.format(func.func_name, args, kwargs))
            ps = pstats.Stats(pr, stream=open(path, 'a')).sort_stats('cumulative')
            ps.print_stats()
            return result
        return wrap
    return decor


def get_total_mem(pid):
    proc = psutil.Process(pid)
    mem = proc.memory_info().rss
    for child in proc.children(recursive=True):
        mem += get_total_mem(child.pid)
    return mem


class MemMonitor(Thread):
    def __init__(self, pid):
        Thread.__init__(self)
        self.max_mem = 0
        self.pid = pid

    def run(self):
        while psutil.pid_exists(self.pid):
            try:
                mem = get_total_mem(self.pid)
                if mem > self.max_mem:
                    self.max_mem = mem
            except psutil.NoSuchProcess:
                break
            except Exception as ex:
                logger.error(ex)
                break

            time.sleep(0.2)


def parallel(func, params):
    tasks = [Task(func) for param in params]
    for ind, param in enumerate(params):
        tasks[ind].start(*param)
    return tasks


def parallel_wait(func, params):
    tasks = parallel(func, params)
    wait_for(tasks)
    return tasks


def wait_for(tasks):
    for task in tasks:
        task.wait()


class Task(object):
    def __init__(self, caller, timeout=None, reserved_mem=None,\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
        stdin=subprocess.PIPE):
        self.timeout = None
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
        self.result = None

    def start(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.start_time = datetime.now()
        self.state = 'Started'
        func_name = None
        if isinstance(self.caller, basestring):
            self.process = subprocess.Popen(self.caller,\
                stdout=self.stdout,\
                stdin=self.stdin,\
                stderr=self.stderr, shell=True)
            self.id = hashlib.md5(self.caller).hexdigest()
            self.func_name = self.caller

        elif self.caller is not None:
            self.id = hashlib.md5('{},{},{}'.format(self.caller.func_name, args, kwargs)).hexdigest()
            self.parent_conn, self.child_conn = Pipe()
            xargs = [self.caller, self.child_conn] + list(args)
            self.process = Process(target=callit, args=xargs, kwargs=kwargs)
            self.process.start()
            self.func_name = self.process._target.func_name

        if self.process is not None:
            logger.info('Task {} PID: {} is running {}{}, keywords:{}'.format(self.id, self.process.pid,\
                self.func_name, args, kwargs))
            self.monitor = MemMonitor(self.process.pid)
            self.monitor.start()

    def wait(self):
        if self.state == 'Created':
            logger.warn('ou have not run the task yet.')

        if self.state != 'Started':
            return

        if self.process is None: # for profiling
            return

        if isinstance(self.process, subprocess.Popen):
            self.out, self.err = self.process.communicate()
            self.monitor.join(1) # end monitoring
            self.end_time = datetime.now()
            self.max_mem = self.monitor.max_mem
            if self.process.returncode != 0:
                self.state = 'Failed'
                raise Exception(self.err)

        else: # Process obj:
            self.process.join()
            res = self.parent_conn.recv()
            logger.info('Task {} id is done.'.format(self.id))
            self.end_time = datetime.now()
            self.max_mem = self.monitor.max_mem
            if res['error'] is not None:
                self.state = 'Failed'
                raise Exception(res['error'])

            if self.process.exitcode != 0:
                self.state = 'Failed'
                raise Exception('ERROR: method failed: {}{},'\
                    ' kewords:{}'.format(\
                    self.func_name, self.args, self.kwargs))

            self.result = res['result']
        self.state = 'Succeeded'


    def get_stats(self):
        self.wait()
        delta = self.end_time - self.start_time
        return {'id': self.id,
            'start': self.start_time.strftime("%H:%M:%S"),
            'end': self.end_time.strftime("%H:%M:%S"),
            'duration': delta.seconds,
            'max_mem': self.max_mem}


class Pipeline(object):
    def __init__(self):
        self.tasks = []

    def parallel_wait(self, func, params):
        for task in parallel_wait(func, params):
            self.tasks.append(task)

    def parallel(self, func, params):
        for task in parallel(func, params):
            self.tasks.append(task)

    def get_stats(self):
        return [task.get_stats() for task in self.tasks]

    def wait(self):
        for task in self.tasks:
            task.wait()

    def results(self):
        self.wait()
        return [task.result for task in self.tasks]

