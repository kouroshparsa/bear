"""
Used as a demo driver for plotting features
copyright: (c) 2014 by Kourosh Parsa.
"""
from bear.pipeline import Pipeline
from time import sleep
import tempfile


def add(a, b):
    sleep(2)
    return a + b


def subtract(a, b):
    sleep(3)
    return a - b


def multiply(a, b):
    sleep(4)
    return a * b


if __name__ == '__main__':
    pipe = Pipeline(memory_monitor_interval=1)
    pipe.parallel_sync(add, [[1, 2], [2, 4], [4, 4]])
    pipe.parallel_sync(subtract, [[1, 2], [2, 4], [4, 4]])
    pipe.parallel_sync(subtract, [[1, 2], [2, 4], [4, 4]])

    with tempfile.NamedTemporaryFile() as tf:
        pipe.plot_tasks_duration(tf.name)
        pipe.plot_tasks_memory(tf.name)
        pipe.plot_system_memory(tf.name)

