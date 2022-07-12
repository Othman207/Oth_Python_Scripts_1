import random


def random_color():
    def r():
        return random.randint(0, 255)
    return ('#%02X%02X%02X' % (r(), r(), r()))
