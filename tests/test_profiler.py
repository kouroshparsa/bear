"""
Validates the profiler
copyright: (c) 2014 by Kourosh Parsa.
"""
import re
import time
import unittest
import tempfile
from bear import profile, Pipeline

OS_LVL, PROFILE_PATH = tempfile.mkstemp()


@profile(PROFILE_PATH)
def go(num):
    """
    num: positive int
    """
    for _ in range(num):
        time.sleep(0.5)


class TestProfiler(unittest.TestCase):
    """ test profiling """

    def test_prof(self):
        pipeline = Pipeline()
        pipeline.parallel_sync(go, [[1], [2]])
        content = open(PROFILE_PATH, 'r').read()
        assert len(re.findall(r'(?=Profiling)', content)) == 2


if __name__ == '__main__':
    unittest.main()
