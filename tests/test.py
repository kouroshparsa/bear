"""
Validates the package
copyright: (c) 2014 by Kourosh Parsa.
"""
import os
import sys
import time
import json
import unittest
import tempfile
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.realpath("{}/..".format(BASE_DIR))
sys.path.append(ROOT)

from bear import Task, Pipeline, TaskError, _get_sub_params


def add(a, b):
    """ adds 2 numbers """
    time.sleep(1)
    return a + b


def subtract(a, b):
    """ subtracts 2 numbers """
    time.sleep(2)
    return a - b


def go(num):
    """
    num: positive int
    """
    for _ in range(num):
        time.sleep(0.5)


def download(url, extract=False):
    if 'url' not in url:
        raise Exception('Invalid url %s' % url)
    if not extract:
        raise Exception('Did not pass kwarg to the method')
    print('downloading %s' % url)


def get_big_data():
    _, temp_file = tempfile.mkstemp()
    with open(temp_file, 'wb') as f:
        f.seek(100 * 1024 * 1024)  # 100 MB
        f.write(b'0')
    res = open(temp_file, 'r').read()
    time.sleep(2)


class TestMem(unittest.TestCase):
    """ tests memory monitoring """
    def test_mem(self):
        pipeline = Pipeline()
        pipeline.parallel_sync(get_big_data, [[], [], []])
        stats = pipeline.get_stats()
        assert len(stats) == 3, f"Invalid number of stats: {stats}"
        for val in stats:
            assert val['max_mem'] > 50 * 1024 * 1024, f"Unexpected max_mem: {val}"


class TestBear(unittest.TestCase):
    """ general functional test """
    @classmethod
    def cleanup(self):
        _, self.path1 = tempfile.mkstemp()
        _, self.path2 = tempfile.mkstemp()

    @classmethod
    def setUpClass(self):
        self.cleanup()

    @classmethod
    def tearDownClass(self):
        self.cleanup()

    def test1(self):
        handle = open(self.path1, 'w')
        tasks = [Task(add, [1, 2]), Task(subtract, [1, 2]), Task('echo hello', stdout=handle)]
        tasks[0].start()
        tasks[1].start()
        tasks[2].start()
        res = [task.wait() for task in tasks]
        print([task.get_stats() for task in tasks])
        assert 'hello' in open(self.path1, 'r').read()

    def test2(self):
        pipe = Pipeline()
        res = pipe.parallel_sync(add, [(1, 1), (1, 2)])
        assert res == [2, 3], f"unexpected result: {res}"

    def test3(self):
        pipe = Pipeline()
        pipe.parallel_async(add, [(1, 1), (2, 2)])
        results = pipe.get_all_results()
        assert results == [2, 4], f"Actual result: {results}"

    def test4(self):
        pipe = Pipeline()
        pipe.parallel_sync(download, [['url1'], ['url2'], ['url3']], {'extract': True})

    def test5(self):
        pipe = Pipeline()
        pipe.parallel_async(subtract, [[2, 1], [5, 4]])
        time.sleep(5)
        stats = pipe.get_stats()
        assert stats[0]['duration'] in [2, 3] and stats[1]['duration'] in [2, 3],\
            'The duration is too off. Actual durations in seconds: {} and {}'.format(\
            stats[0]['duration'], stats[1]['duration'])

    def test6(self):
        pipe = Pipeline()
        with self.assertRaises(TaskError):
            pipe.parallel_sync(subtract, [[1, 2], [1, 'x']])

    def test7(self):
        pipe = Pipeline()
        pipe.parallel_sync(subtract, [[1, 2], [-1, -2]])
        pipe.save_stats(self.path2)
        stats = json.load(open(self.path2))
        assert isinstance(stats[0]['duration'], int)

    def test8(self):
        """ test limited concurrency """
        pipe = Pipeline()
        pipe.parallel_sync(subtract, [[1, 2], [1, 1], [1, 3]], concurrency=2)
        assert pipe.get_all_results() == [-1, 0, -2]

    def test9(self):
        assert _get_sub_params([['a', 1], ['b', 1], ['c', 1]], 3) == [[['a', 1], ['b', 1], ['c', 1]]]
        assert _get_sub_params([['a', 1], ['b', 1], ['c', 1]], 2) == [[['a', 1], ['b', 1]], [['c', 1]]]


class TestPlots(unittest.TestCase):
    """ test plotting functions """
    def test_duration_plot(self):
        pipe = Pipeline()
        pipe.parallel_sync(subtract, [[1, 2], [-1, -2]])
        pipe.parallel_sync(add, [[1, 2], [-1, -2]])
        _, path = tempfile.mkstemp()
        pipe.plot_tasks_duration(path)
        _, path = tempfile.mkstemp()
        pipe.plot_tasks_memory(path)


if __name__ == '__main__':
    unittest.main()
