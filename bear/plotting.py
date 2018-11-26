import matplotlib
matplotlib.use('agg') # run headless
# with python3, I get an error because matplotlib need tkinter
# but I'm not using it so I can run headless
import matplotlib.pyplot as plt
from matplotlib.path import Path
import matplotlib.patches as patches
from datetime import datetime

def draw_path(start, end, y, thickness, ax):
    verts = [
       (start, y),  # left, bottom
       (start, y+thickness),  # left, top
       (end, y+thickness),  # right, top
       (end, y),  # right, bottom
       (start, y),  # ignored
    ]

    codes = [
        Path.MOVETO,
        Path.LINETO,
        Path.LINETO,
        Path.LINETO,
        Path.CLOSEPOLY,
    ]

    path = Path(verts, codes)
    patch = patches.PathPatch(path, facecolor='orange', lw=2)
    ax.add_patch(patch)
    mid = (end + start) / 2.0
    duration = "{} Hour".format(end - start)
    ax.annotate(duration, xy=(mid, y-0.2), xycoords='data',
        xytext=(mid, y-0.2), textcoords='data')
    mem = "{} GB".format(thickness)
    ax.annotate(mem, xy=(end, y+thickness/2), xycoords='data',
        xytext=(end, y+thickness/2), textcoords='data')


def draw_intervals(path, stats):
    fig, ax = plt.subplots()
    ax.set_xlim(-2, 4)
    ax.set_ylim(-2, 4)
    for stat in stats:
        draw(0, 3, 1, 0.4, ax)
    plt.savefig(path)


def plot_tasks(tasks, path):
    # sort task by start time:
    tasks = sorted(tasks,
        key=lambda x: datetime.strptime(x.start_time, '%m/%d/%y %H:%M'), reverse=True)
    # TODO


def plot_tasks_duration(tasks, path):
    # sort task by start time:
    tasks = sorted(tasks,
        key=lambda x: x.start_time, reverse=True)
    x = range(len(tasks))
    labels = [task.func_name for task in tasks]
    y = [(task.end_time - task.start_time).seconds for task in tasks]
    plt.bar(x, y)
    plt.xticks(x, labels, rotation=45)
    plt.ylabel('Duration in seconds')
    plt.savefig(path)


def plot_tasks_memory(tasks, path):
    # sort task by start time:
    tasks = sorted(tasks,
        key=lambda x: x.start_time, reverse=True)

    x = range(len(tasks))
    labels = [task.func_name for task in tasks]
    y = [task.max_mem for task in tasks]
    plt.xticks(x, labels, rotation=45)
    plt.ylabel('Max RSS memory used')
    plt.bar(x, y)
    plt.savefig(path)

