from bear import parallel


def calculate(a, b, c=2):
    return (a + b) / c


if __name__ == '__main__':
    tasks = parallel(calculate, [(1, 2), (2, 3), (5, 5)], c=5)
    print([task.get_result() for task in tasks])
