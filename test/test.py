"""
Validates the package
copyright: (c) 2014 by Kourosh Parsa.
"""
import os
import sys
import time
import re
import unittest
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.realpath("{}/..".format(BASE_DIR))
sys.path.append(ROOT)

from bear import Task, profile, parallel_wait, Pipeline
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
        pipe.parallel(add, [(1,2)])
        pipe.parallel(subtract, [(1,2)])
        stats = {}
        for val in pipe.get_stats():
            stats[val['id']] = val['max_mem']
        assert stats['94a3f806fd092fdfd5d9a1f7ad8eaf0f'] > 200 * 1024 * 1024, 'Got:\n{}'.format(stats)
        assert stats['fdd8ed4916c6e975c24085bc2b3df67a'] < 35 * 1024 * 1024, 'Got:\n{}'.format(stats)

    @classmethod
    def tearDownClass(self):
        os.remove(BIG_FILE_PATH)


class TestBear(unittest.TestCase):
    """ general functional test """
    @classmethod
    def setUpClass(self):
        self.path = '/tmp/bear.txt'
        if os.path.exists(self.path):
            os.remove(self.path)

    def test1(self):
        handle = open(self.path, 'w')
        tasks = [Task(add), Task(subtract), Task('echo hello', stdout=handle)]
        tasks[0].start(1, 2)
        tasks[1].start(1, 2)
        tasks[2].start()
        res = [task.wait() for task in tasks]
        print([task.get_stats() for task in tasks])
        assert 'hello' in open(self.path, 'r').read()
        os.remove(self.path)


    def test2(self):
        tasks = parallel_wait(add, [(1,1), (1,2)])
        assert tasks[0].result == 2
        assert tasks[1].result == 3

    def test3(self):
        pipe = Pipeline()
        pipe.parallel(add, [(1,1), (2,2)])
        results = pipe.results()
        assert results == [2, 4]

    def test4(self):
        pipe = Pipeline()
        pipe.parallel_wait(download, [['url1'], ['url2'], ['url3']], extract=True)

    def test5(self):
        pipe = Pipeline()
        pipe.parallel(subtract, [[2, 1], [5, 4]])
        time.sleep(5)
        stats = pipe.get_stats()
        assert stats[0]['duration'] == stats[1]['duration'] == 2,\
            'The duration is wrong. Expected 2, Actual {} and {}'.format(\
            2, stats[0]['duration'], stats[1]['duration'])


if __name__ == '__main__':
    unittest.main()
