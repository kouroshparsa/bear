"""
Used as a demo driver for plotting features
copyright: (c) 2014 by Kourosh Parsa.
"""
import os
from bear.pipeline import Pipeline
from time import sleep
import tempfile
import random


def big_mem(num_of_bytes, count):
    arr = []
    for _ in range(count):
        arr.append(random.randbytes(num_of_bytes))
        sleep(0.1)
    return ''


def small_mem(count):
    arr = []
    for _ in range(count):
        arr.append(random.randint(1, 100))
        sleep(0.1)
    return ''


def long_call(count):
    for _ in range(count):
        sleep(0.1)
    return ''


def serial(num_of_bytes, count):
    arr = []
    for _ in range(count):
        arr.append(random.randbytes(num_of_bytes))
        sleep(0.1)
    return ''


if __name__ == '__main__':
    pipe = Pipeline(memory_monitor_interval=1)
    pipe.parallel_async(big_mem, [[5000, 20], [2000, 18], [4000, 14]])
    pipe.parallel_async(small_mem, [[12], [14], [16]])
    pipe.parallel_async(long_call, [[30], [25]])
    pipe.wait()
    pipe.parallel_sync(serial, [[2000, 13], [4000, 10]])

    dir_path = None
    with tempfile.NamedTemporaryFile() as tf:
        dir_path = os.path.dirname(tf.name)

    pipe.plot_tasks_duration(os.path.join(dir_path, 'duration.png'))
    pipe.plot_tasks_memory(os.path.join(dir_path, 'task_memory.png'))
    pipe.plot_system_memory(os.path.join(dir_path, 'system_memory.png'))
    pipe.save_stats(os.path.join(dir_path, 'stats.json'))
    print('Saved to: ', dir_path)
