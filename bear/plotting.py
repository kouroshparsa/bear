"""
This module is used for plotting pipeline statistics and diagrams
"""
import matplotlib
from collections import OrderedDict

matplotlib.use('agg')  # run headless
# with python3, I get an error because matplotlib needs tkinter
# but I'm not using it so I can run headless
import matplotlib.pyplot as plt
from matplotlib import dates as mdates

BAR_WIDTH = 0.25  # the width of the bars


def __get_grouped_data(tasks, data_type):
    """ Gets a specific data from tasks grouped together
    :param tasks: list of Task objects
    :para, data_type: string, can be either duration or memory
    :return: a dict
    """
    assert data_type in ['duration', 'memory']
    groups = {}
    for task in tasks:
        if task.group_id not in groups:
            groups[task.group_id] = {}

        method = getattr(task, 'get_' + data_type)
        if task.func_name in groups[task.group_id]:
            groups[task.group_id][task.func_name].append(method())

        else:
            groups[task.group_id][task.func_name] = [method()]
    return groups


def __plot_task_dist(ax, tasks):
    """ Adds the second subplot which displays memory usage and duration
    :param ax: matplotlib axis
    :param tasks: list of Task objects
    :return: None
    """
    tasks = sorted(tasks, key=lambda x: x.start_time)
    ax.xaxis_date()
    ax.grid(True)
    colors = plt.cm.tab10(range(len(tasks)))
    mem_level = 0
    color_ind = 0
    legend_colors = {}
    for task in tasks:
        m_start = mdates.date2num(task.start_time)
        m_end = mdates.date2num(task.end_time)
        color = None
        if task.func_name in legend_colors:
            color = legend_colors[task.func_name]
        else:
            color = colors[color_ind]
            legend_colors[task.func_name] = color
            color_ind += 1

        ax.broken_barh([(m_start, m_end - m_start)], (mem_level, task.max_mem),
                       facecolor=color, label=task.func_name, edgecolor='black')

        mem_level += task.max_mem

    ax.set_ylim([0, mem_level])
    ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1))
    # make the legend labels unique:
    handles, labels = ax.get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), loc='upper left', bbox_to_anchor=(1.05, 1))


def plot_system_memory(path, tasks, sys_mem, width=10, height=6):
    """ Plots system memory usage and individual tasks run and saves the plot to a file
    :param path: absolute path of the output file
    :param tasks: list of Task objects
    :param sys_mem: list of tuples (datetime, memory as int)
    :param width: int width of the plot
    :param height: int height of the plot
    :return: None
    """
    if len(tasks) < 1:
        return

    x = [mdates.date2num(val.timestamp) for val in sys_mem]
    y = [val.percent_mem_used for val in sys_mem]
    fig, ax = plt.subplots(figsize=(width, height))
    ax1 = plt.subplot(211)
    plt.plot(x, y)
    ax1.xaxis_date()
    ax2 = plt.subplot(212)
    __plot_task_dist(ax2, tasks)

    plt.gcf().autofmt_xdate()
    plt.xlim([x[0], x[-1]])

    plt.xlabel('Time')
    ax1.set_ylabel('Percent used Memory')
    plt.savefig(path, bbox_inches='tight')


def plot_tasks_duration(tasks, path, width=10, height=6):
    """ Plots tasks durations and saves the plot to a file
    :param tasks: list of Task objects
    :param path: path of the image to be saved
    :param width: int width of the plot
    :param height: int height of the plot
    :return: None
    """
    groups = __get_grouped_data(tasks, 'duration')
    fig, ax = plt.subplots(figsize=(width, height))

    ind = 0
    for group_id, task in groups.items():
        for func_name, durations in task.items():
            x = [ind + (BAR_WIDTH * multiplier) for multiplier, _ in enumerate(durations)]
            ax.bar(x, durations, BAR_WIDTH, label=func_name)
        ind += 1

    ax.set_ylabel('Duration in seconds')
    ax.set_title('Duration of Tasks')
    ax.set_xticks([])
    ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1))
    plt.savefig(path, bbox_inches='tight')


def plot_tasks_memory(tasks, path, width=10, height=6):
    """ Plots individual tasks' memory usage and saves the plot to a file
    :param tasks: list of Task objects
    :param path: path of the image to be saved
    :param width: int width of the plot
    :param height: int height of the plot
    :return: None
    """
    groups = __get_grouped_data(tasks, 'memory')
    fig, ax = plt.subplots(figsize=(width, height))

    ind = 0
    for group_id, task in groups.items():
        for func_name, memory in task.items():
            x = [ind + (BAR_WIDTH * multiplier) for multiplier, _ in enumerate(memory)]
            ax.bar(x, memory, BAR_WIDTH, label=func_name)
        ind += 1

    ax.set_ylabel('Memory')
    ax.set_title('Max memory usage of tasks')
    ax.set_xticks([])
    ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1))
    plt.savefig(path, bbox_inches='tight')
