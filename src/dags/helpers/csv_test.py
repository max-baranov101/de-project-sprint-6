from itertools import islice


def test_csv(*, file: str):
    with open(file, "r") as f:
        for line in islice(f, 0, 9):
            print(line.rstrip())
