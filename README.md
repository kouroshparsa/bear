Bear
==========

bear is a package for running tasks asynchronously and in parallel in such as way that you can actually use the cpu cores.
`bear` can also be used as a pipeline where you can monitor tasks durations and memory usage and plot them.
The reason why I created this package was that other pipeline packages such as celery proved to be very unreliable.

How to install:
`pip install bear`


Examples:
```
from bear import Task, parallel_wait

def add(a, b):
    return a + b

task = Task(add)
task.start(1, 2)
>>> print task.get_stats()
{'duration': 0, 'start': '01:08:07', 'end': '01:08:07', 'id': '94a3f806fd092fdfd5d9a1f7ad8eaf0f', 'max_mem': 0}


tasks = sync(add, [(1,2), (2,3), (5,5)])
>>> print tasks[0].result, tasks[1].result, tasks[2].result
3 5 10
```

Here is an example for using bear as a pipeline:
```
from bear import Pipeline
def add(a, b):
    return a + b

pipe = Pipeline()
pipe.sync(add, [(1,1), (1,2)])
>>> print pipe.results()
[2, 3]
```
The pipeline has the ability to resume when it fails. In order to do so, you need to enable that feature as in the example below:
```
from bear import Pipeline
def div(a, b):
    return a / b

pipe = Pipeline(resume=True)
```
results = pipe.sync(div, [(1, 1), (1, 0)])
This fails because you are dividing by zero. Then you can modify your div method and re-run the pipeline.
The pipeline already know that you successfully divided 1 by 1 but you failed to devide 1 by zero, so next time you run
 the pipeline, it will skip the successful task since you turned on "resume".




You can also profile your tasks:
```
from bear import profile, Pipeline
@profile('/tmp/add.prof')
def add(a, b):
    return a + b

pipe = Pipeline()
pipe.sync(add, [(1,1), (1,2)])
```
This will create the profiling file /tmp/add.prof and put 2 profiling data for each scenario allowing you to compare results

You can run the pipeline asynchronously using the async method.
You can also specify the concurrency when calling the sync method. For example, if you have 50 tasks and you want to run only 10 at the time to avoid memory allocation problems, you can run it this way:
```
results = sync(add, my_params, concurrency=10)
print(results)
```

Lastly you can also monitor the total system memory and plot it along with the memory used by the tasks.
To do so, refer to the sample file in tests/test_plot.py
 