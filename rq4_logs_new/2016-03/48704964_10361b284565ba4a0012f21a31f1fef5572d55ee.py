import os

def gen_path(filename):
    return os.path.join(os.path.dirname(__file__), filename)