"""
Validates the package
copyright: (c) 2014 by Kourosh Parsa.
"""
import os
import sys
import time
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.realpath("{}/..".format(BASE_DIR))
sys.path.append(ROOT)
import unittest
import re

from bear import Task, profile, parallel_wait, Pipeline
BIG_FILE_PATH = '/tmp/big_bear.txt'
PROFILE_PATH = '/tmp/prof'

def add(a, b):
    if os.path.exists(BIG_FILE_PATH):
        x = open(BIG_FILE_PATH, 'r').read()
    time.sleep(1)
    return a + b

def subtract(a, b):
    time.sleep(2)
    return a - b

@profile(PROFILE_PATH)
def go(a):
    for i in range(a):
        time.sleep(0.5)


class TestProfiler(unittest.TestCase):
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
    @classmethod
    def setUpClass(self):
        if os.path.exists(BIG_FILE_PATH):
            os.remove(BIG_FILE_PATH)

        with open(BIG_FILE_PATH, 'wb') as handle:
            handle.truncate(100 * 1024 * 1024)

    def test_mem(self):
        pipe = Pipeline()
        pipe.parallel(add, [(1,2)])
        pipe.parallel(subtract, [(1,2)])
        stats = {}
        for val in pipe.get_stats():
            stats[val['id']] = val['max_mem']
        assert stats['94a3f806fd092fdfd5d9a1f7ad8eaf0f'] > 100 * 1024 * 1024
        assert stats['fdd8ed4916c6e975c24085bc2b3df67a'] < 10 * 1024 * 1024

    @classmethod
    def tearDownClass(self):
        os.remove(BIG_FILE_PATH)


class TestBear(unittest.TestCase):
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
        print [task.get_stats() for task in tasks]
        assert 'hello' in open(self.path, 'r').read()
        os.remove(self.path)


    def test2(self):
        tasks = parallel_wait(add, [(1,1), (1,2)])
        assert tasks[0].result==2
        assert tasks[1].result==3

    def test3(self):
        pipe = Pipeline()
        pipe.parallel(add, [(1,1), (1,2)])
        results = pipe.results()
        assert results==[2, 3]

if __name__ == '__main__':
    unittest.main()
