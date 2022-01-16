"""
Used as a demo driver for plotting features
copyright: (c) 2014 by Kourosh Parsa.
"""
from bear import Pipeline
from time import sleep


def add(a, b):
    sleep(2)
    return a + b


def subtract(a, b):
    sleep(3)
    return a - b


def multiply(a, b):
    sleep(4)
    return a * b


pipe = Pipeline(memory_monitor_interval=1)
pipe.parallel_sync(add, [[1, 2], [2, 4], [4, 4]])
pipe.parallel_sync(subtract, [[1, 2], [2, 4], [4, 4]])
pipe.parallel_sync(subtract, [[1, 2], [2, 4], [4, 4]])

pipe.plot_tasks_duration('/tmp/duration.png')
pipe.plot_tasks_memory('/tmp/memory.png')
pipe.plot_system_memory('/tmp/sys_memory.png')

