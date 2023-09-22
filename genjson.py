import random
import string
import json


def random_str(size):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size))


def random_dict(nest=0):
    d = {}
    if nest > 3:
        return d

    for count in range(128):
        nest += 1
        d[random_str(8)] = random_dict(nest)

    return d

def main():

    print(json.dumps(random_dict()))


if __name__ == '__main__':
    main()
