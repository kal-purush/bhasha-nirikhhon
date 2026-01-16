import pytest

from jsonrpc.exceptions import JSONRPCDispatchException
from core_rover.server.core.utils import (
    InvalidParametersException,
    is_strict_int,
)

class TestUtils(object):

    def test_invalid_parameters_exception_subclass(self):
        assert issubclass(InvalidParametersException, JSONRPCDispatchException)

    def test_invalid_parameters_exception_data(self):
        exception = InvalidParametersException()
        assert exception.code == -32602
        assert exception.message == "Invalid params"

    @pytest.mark.parametrize(
        argnames='data',
        argvalues=[
            222, -222, 0,
        ],
    )
    def test_is_strict_int(self, data):
        assert is_strict_int(value=data)

    @pytest.mark.parametrize(
        argnames='data',
        argvalues=[
            1234.23, True, False, '', [], {}, -0.43,
        ],
    )
    def test_is_strict_int_incorrect_value(self, data):
        assert not is_strict_int(value=data)