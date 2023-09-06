import os
import time
import json
from bear import Task, State, DELAY, SystemMonitor, plotting


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

            #time.sleep(DELAY)  # necessary to avoid deadlock

    def parallel_sync(self, func, args, kwargs={}, concurrency=1000):
        """
        :param func: function signature
        :param args: list
        :param concurrency: int
        :param kwargs: dictionary
        :return: list of results
        runs tasks in parallel and waits for them to finish
        """
        tasks = self.__create_tasks(func, args, kwargs)
        self.__start_tasks(tasks, concurrency=concurrency)
        for task in tasks:
            task.wait()

        return [task.result for task in tasks]

    def parallel_async(self, func, args, kwargs={}):
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
            raise Exception('There is no system memory usage data. ' \
                            'You need to turn it on when creating a Pipeline.')
        plotting.plot_system_memory(path, self.tasks, sys_mem)
