from datetime import datetime
import matplotlib
matplotlib.use('agg') # run headless
# with python3, I get an error because matplotlib need tkinter
# but I'm not using it so I can run headless
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import matplotlib.dates as mdates
import matplotlib.colors as colors
import matplotlib.cm as cmx


def plot_system_memory(tasks, path, sys_mem):
    """
    :param tasks: list of Task objects
    :param path: absolute oath of the output file
    :param sys_mem: list of tuples (datetime, memory as int)
    saves the plot to a file
    """
    if len(tasks) < 1:
        return

    x = [mdates.date2num(val.timestamp) for val in sys_mem]
    y = [val.percent_mem_used for val in sys_mem]
    ax1 = plt.subplot(211)
    plt.plot(x, y)
    ax1.xaxis_date()
    ax2 = plt.subplot(212)
    ax2.xaxis_date()
    jet = plt.get_cmap('jet')
    norm  = colors.Normalize(vmin=0, vmax=len(tasks))
    scalar_map = cmx.ScalarMappable(norm=norm, cmap=jet)
    task_groups = {}
    for task in tasks:
        if task.func_name in task_groups:
            task_groups[task.func_name].append(task)
        else:
            task_groups[task.func_name] = [task]

    color_ind = 0
    max_task_mem = 0
    prev_group_id = tasks[0].group_id
    for name, tasks in task_groups.items():
        color = scalar_map.to_rgba(color_ind)
        color_ind += 1
        bottom = 0
        for task in tasks:
            if task.group_id != prev_group_id:
                bottom = 0

            m_start = mdates.date2num(task.start_time)
            m_end = mdates.date2num(task.end_time)
            width = m_end - m_start
            rect = Rectangle((m_start, bottom), width, task.max_mem, color=color, alpha=0.3)
            ax2.add_patch(rect)
            border = Rectangle((m_start, bottom), width, task.max_mem, color='black', fill=False)
            ax2.add_patch(border)
            plt.annotate(task.func_name, (m_start + width/2, task.max_mem + bottom))
            bottom += task.max_mem
            max_task_mem = max(max_task_mem, bottom)
            prev_group_id = task.group_id

    plt.gcf().autofmt_xdate()
    plt.xlim([x[0], x[-1]])
    ax2.set_ylim([0, max_task_mem * 1.1])
    plt.xlabel('Time')
    ax1.set_ylabel('Percent used Memory')
    ax2.set_ylabel('Memory')
    plt.tight_layout()
    plt.savefig(path)


def plot_tasks_duration(tasks, path):
    """
    :param tasks: list of Task objects
    :param path: absolute path of the image to be saved
    sorts task by start time and plots the duration on the y axis and
    the x axis of the bar chart is the tasks in the order of execution
    """
    tasks = sorted(tasks,
                   key=lambda x: x.start_time)
    x = range(len(tasks))
    labels = [task.func_name for task in tasks]
    y = [(task.end_time - task.start_time).seconds for task in tasks]
    plt.bar(x, y)
    plt.xticks(x, labels, rotation=45)
    plt.ylabel('Duration in seconds')
    plt.tight_layout()
    plt.savefig(path)


def plot_tasks_memory(tasks, path):
    """
    :param tasks: list of Task objects
    :param path: absolute path of the image to be saved
    sorts task by start time and plots memory usage on the y axis and
    the x axis of the bar chart is the tasks in the order of execution
    """
    tasks = sorted(tasks,
                   key=lambda x: x.start_time)

    x = range(len(tasks))
    labels = [task.func_name for task in tasks]
    y = [task.max_mem for task in tasks]
    plt.xticks(x, labels, rotation=45)
    plt.ylabel('Max RSS memory used')
    plt.bar(x, y)
    plt.tight_layout()
    plt.savefig(path)

