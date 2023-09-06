from bear.pipeline import Pipeline


def add(a, b):
    return a + b


if __name__ == '__main__':
    pipe = Pipeline()
    pipe.parallel_sync(add, [(1, 1), (1, 2)])
