#encoding: utf-8

from nose.tools import *
import sys
sys.path.append('../lib')

from filters import exclude

@raises(TypeError)
def test_takes_a_list_and_object_as_argument():
    exclude(1337, 45)
    exclude()
    exclude("kjkj")
    exclude([1, 2, 3])

def test_should_return_list():
    result = exclude([1, 2, 3], 4)

    # result should  be a list
    assert_true(isinstance(result, list))


def test_should_return_list_without_items():
    result = exclude(["bosse", "daniel", "edvard", "bosse", "bosse"], "bosse")

    length = len(result) # should be 2


    for name in result:
        assert_true(name != "bosse")

    assert_equal(length, 2)