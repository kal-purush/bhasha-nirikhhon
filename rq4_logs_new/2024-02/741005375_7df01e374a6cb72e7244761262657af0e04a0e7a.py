from graphdiffusion.pipeline import *

import pytest 
from torch import nn 



def func(x879879789789789=1):
    return x879879789789789

def test_func():
    config = get_config()
    params = get_params(func, config)
    assert "x879879789789789" not in params

def test_func2():
    config = get_config()
    config["x879879789789789"] = 2
    params = get_params(func, config)
    assert params["x879879789789789"] == 2
    assert func(**params) == 2

def test_func3():
    config = get_config()
    config["x879879789789789"] = 2
    params = get_params(func, config, {"x879879789789789": 3})
    assert params["x879879789789789"] == 3
    assert func(**params) == 3


class CallableClass:
    def __init__(self):
        pass
    
    def __call__(self, x879879789789789=1):
        return x879879789789789



def test_class():
    c = CallableClass()
    config = get_config()
    params = get_params(c, config)
    assert "x879879789789789" not in params

def test_class2():
    config = get_config()
    c = CallableClass()
    config["x879879789789789"] = 2
    params = get_params(c, config)
    assert params["x879879789789789"] == 2
    assert c(**params) == 2

def test_class3():
    c = CallableClass()
    config = get_config()
    config["x879879789789789"] = 2
    params = get_params(c, config, {"x879879789789789": 3})
    assert params["x879879789789789"] == 3
    assert c(**params) == 3