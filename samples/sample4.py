from bear.pipeline import Pipeline


def div(a, b):
    return a / b


if __name__ == '__main__':
    pipe = Pipeline()
    results = pipe.parallel_sync(div, [(1, 1), (1, 9)])
    print(results)
