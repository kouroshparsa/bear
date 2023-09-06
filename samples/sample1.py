from bear import Task


def add(a, b):
    return a + b


if __name__ == '__main__':
    task = Task(add)
    task.start(args=[1, 2])
    print('Result:', task.get_result())
    print(task.get_stats())
