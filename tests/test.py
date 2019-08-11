"""
Validates the package
copyright: (c) 2014 by Kourosh Parsa.
"""
import os
import sys
import time
import re
import json
import unittest
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.realpath("{}/..".format(BASE_DIR))
sys.path.append(ROOT)

from bear import Task, profile,\
    Pipeline, TaskError, _get_sub_params
BIG_FILE_PATH = '/tmp/big_bear.txt'
PROFILE_PATH = '/tmp/prof'


def add(a, b):
    """ adds 2 numbers """
    if os.path.exists(BIG_FILE_PATH):
        open(BIG_FILE_PATH, 'r').read()
    time.sleep(1)
    return a + b


def subtract(a, b):
    """ subtracts 2 numbers """
    time.sleep(2)
    return a - b


@profile(PROFILE_PATH)
def go(a):
    """ a method to profile """
    for _ in range(a):
        time.sleep(0.5)


def download(url, extract=False):
    if 'url' not in url:
        raise Exception('Invalid url %s' % url)
    if not extract:
        raise Exception('Did not pass kwarg to the method')
    print('downloading %s' % url)


class TestProfiler(unittest.TestCase):
    """ test profiling """
    @classmethod
    def setUpClass(cls):
        if os.path.exists(PROFILE_PATH):
            os.remove(PROFILE_PATH)

    def test_prof(self):
        pipeline = Pipeline()
        pipeline.sync(go, [[1], [2]])
        assert os.path.exists(PROFILE_PATH)

    def test1_prof(self):
        pipeline = Pipeline()
        pipeline.sync(go, [[1], [2]])
        content = open(PROFILE_PATH, 'r').read()
        assert len(re.findall(r'(?=Profiling)', content))==2


# class TestMem(unittest.TestCase):
#     """ tests memory monitoring """
#     @classmethod
#     def setUpClass(cls):
#         if os.path.exists(BIG_FILE_PATH):
#             os.remove(BIG_FILE_PATH)
#
#         with open(BIG_FILE_PATH, 'wb') as handle:
#             handle.truncate(200 * 1024 * 1024)
#
#     def test_mem(self):
#         pipeline = Pipeline()
#         pipeline.async(add, [(1,2)])
#         pipeline.async(subtract, [(1,2)])
#         stats = {}
#         for val in pipeline.get_stats():
#             stats[val['id']] = val['max_mem']
#         print stats.keys
#         assert stats['94a3f806fd092fdfd5d9a1f7ad8eaf0f'] > 200 * 1024 * 1024, 'Got:\n{}'.format(stats)
#         assert stats['fdd8ed4916c6e975c24085bc2b3df67a'] < 50 * 1024 * 1024, 'Got:\n{}'.format(stats)
#
#     @classmethod
#     def tearDownClass(cls):
#         os.remove(BIG_FILE_PATH)


class TestBear(unittest.TestCase):
    """ general functional test """
    @classmethod
    def cleanup(cls):
        cls.path1 = '/tmp/bear.txt'
        if os.path.exists(cls.path1):
            os.remove(cls.path1)

        cls.path2 = '/tmp/bear.stat'
        if os.path.exists(cls.path2):
            os.remove(cls.path2)

    @classmethod
    def setUpClass(cls):
        cls.cleanup()

    @classmethod
    def tearDownClass(cls):
        cls.cleanup()

    def test1(self):
        handle = open(self.path1, 'w')
        tasks = [Task(add, [1, 2]), Task(subtract, [1, 2]), Task('echo hello', stdout=handle)]
        tasks[0].start()
        tasks[1].start()
        tasks[2].start()
        res = [task.wait() for task in tasks]
        print([task.get_stats() for task in tasks])
        assert 'hello' in open(self.path1, 'r').read()
        os.remove(self.path1)

    def test2(self):
        pipe = Pipeline()
        res = pipe.sync(add, [(1, 1), (1, 2)])
        assert res == [2, 3]

    def test3(self):
        pipe = Pipeline()
        pipe.async(add, [(1,1), (2,2)])
        results = pipe.get_all_results()
        assert results == [2, 4]

    def test4(self):
        pipe = Pipeline()
        pipe.sync(download, [['url1'], ['url2'], ['url3']], {'extract': True})

    def test5(self):
        pipe = Pipeline()
        pipe.async(subtract, [[2, 1], [5, 4]])
        time.sleep(5)
        stats = pipe.get_stats()
        assert stats[0]['duration'] == stats[1]['duration'] == 2,\
            'The duration is wrong. Expected 2, Actual {} and {}'.format(\
            stats[0]['duration'], stats[1]['duration'])

    def test6(self):
        pipe = Pipeline()
        with self.assertRaises(TaskError):
            pipe.sync(subtract, [[1, 2], [1, 'x']])

    def test7(self):
        pipe = Pipeline()
        pipe.sync(subtract, [[1, 2], [-1, -2]])
        pipe.save_stats(self.path2)
        stats = json.load(open(self.path2))
        assert isinstance(stats[0]['duration'], int)

    def test8(self):
        """ test limited concurrency """
        pipe = Pipeline()
        pipe.sync(subtract, [[1, 2], [1, 1], [1, 3]], concurrency=2)
        assert pipe.get_all_results() == [-1, 0, -2]

    def test9(self):
        assert _get_sub_params([['a', 1], ['b', 1], ['c', 1]], 3) == [[['a', 1], ['b', 1], ['c', 1]]]
        assert _get_sub_params([['a', 1], ['b', 1], ['c', 1]], 2) == [[['a', 1], ['b', 1]], [['c', 1]]]


class TestPlots(unittest.TestCase):
    """ test plotting functions """
    def test_duration_plot(self):
        pipe = Pipeline()
        pipe.sync(subtract, [[1, 2], [-1, -2]])
        pipe.sync(add, [[1, 2], [-1, -2]])
        pipe.plot_tasks_duration('/tmp/x.png')
        pipe.plot_tasks_memory('/tmp/y.png')


if __name__ == '__main__':
    unittest.main()
