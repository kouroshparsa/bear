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

from bear import Task, profile, parallel_wait,\
    Pipeline, TaskError, _get_sub_params
BIG_FILE_PATH = '/tmp/big_bear.txt'
PROFILE_PATH = '/tmp/prof'

def add(a, b):
    """ adds 2 numbers """
    if os.path.exists(BIG_FILE_PATH):
        x = open(BIG_FILE_PATH, 'r').read()
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
    def setUpClass(self):
        if os.path.exists(PROFILE_PATH):
            os.remove(PROFILE_PATH)

    def test_prof(self):
        parallel_wait(go, [[1], [2]])
        assert os.path.exists(PROFILE_PATH)

    def test1_prof(self):
        parallel_wait(go, [[1], [2]])
        content = open(PROFILE_PATH, 'r').read()
        assert len(re.findall(r'(?=Profiling)', content))==2


class TestMem(unittest.TestCase):
    """ tests memory monitoring """
    @classmethod
    def setUpClass(self):
        if os.path.exists(BIG_FILE_PATH):
            os.remove(BIG_FILE_PATH)

        with open(BIG_FILE_PATH, 'wb') as handle:
            handle.truncate(200 * 1024 * 1024)

    def test_mem(self):
        pipe = Pipeline()
        pipe.async(add, [(1,2)])
        pipe.async(subtract, [(1,2)])
        stats = {}
        for val in pipe.get_stats():
            stats[val['id']] = val['max_mem']
        assert stats['94a3f806fd092fdfd5d9a1f7ad8eaf0f'] > 200 * 1024 * 1024, 'Got:\n{}'.format(stats)
        assert stats['fdd8ed4916c6e975c24085bc2b3df67a'] < 50 * 1024 * 1024, 'Got:\n{}'.format(stats)

    @classmethod
    def tearDownClass(self):
        os.remove(BIG_FILE_PATH)


class TestBear(unittest.TestCase):
    """ general functional test """
    @classmethod
    def cleanup(self):
        self.path1 = '/tmp/bear.txt'
        if os.path.exists(self.path1):
            os.remove(self.path1)

        self.path2 = '/tmp/bear.stat'
        if os.path.exists(self.path2):
            os.remove(self.path2)

    @classmethod
    def setUpClass(self):
        self.cleanup()

    @classmethod
    def tearDownClass(self):
        self.cleanup()

    def test1(self):
        handle = open(self.path1, 'w')
        tasks = [Task(add), Task(subtract), Task('echo hello', stdout=handle)]
        tasks[0].start(1, 2)
        tasks[1].start(1, 2)
        tasks[2].start()
        res = [task.wait() for task in tasks]
        print([task.get_stats() for task in tasks])
        assert 'hello' in open(self.path1, 'r').read()
        os.remove(self.path1)


    def test2(self):
        tasks = parallel_wait(add, [(1,1), (1,2)])
        assert tasks[0].result == 2
        assert tasks[1].result == 3

    def test3(self):
        pipe = Pipeline()
        pipe.async(add, [(1,1), (2,2)])
        results = pipe.results()
        assert results == [2, 4]

    def test4(self):
        pipe = Pipeline()
        pipe.sync(download, [['url1'], ['url2'], ['url3']], extract=True)

    def test5(self):
        pipe = Pipeline()
        pipe.async(subtract, [[2, 1], [5, 4]])
        time.sleep(5)
        stats = pipe.get_stats()
        assert stats[0]['duration'] == stats[1]['duration'] == 2,\
            'The duration is wrong. Expected 2, Actual {} and {}'.format(\
            2, stats[0]['duration'], stats[1]['duration'])


    def test6(self):
        pipe = Pipeline()
        with self.assertRaises(TaskError) as context:
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
        assert pipe.results() == [-1, 0, -2]

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
