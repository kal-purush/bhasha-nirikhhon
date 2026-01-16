from itertools import chain, repeat
from unittest.mock import call, Mock
from typing import Any, Callable

from .exception import AllowsException
from .side_effect import SideEffectBuilder


class MockExtensionGrammar:
    """
    MockExtensionGrammar is created by the ``allow`` factory.

    This enables grammar for creating and binding a mock side effect like:

    *allow* **<Mock>** *to* **<Have Side Effect>**
    """

    def __init__(self, mock_subject: Mock) -> None:
        self._mock_subject = mock_subject

    def to(self, side_effect_builder_grammar: "SideEffectBuilderGrammar") -> Mock:
        return side_effect_builder_grammar.apply_to(self._mock_subject)


class SideEffectBuilderGrammar:
    """
    SideEffectBuilderGrammar is initiated by the ``return_value``, ``raise_exception``, ``recieve_method``, ``be_called_with``, ``have_effect`` factory methods.

    The grammar is chainable, but a side effect can have only one effect (return, exception, effect) per grammar expression. However, side effects will automatically combine if multiple expressions are applied to the same mock/method.

    This enables grammar for building the side effect like:

    *allow* **<Mock>** *to* ...

    *be_called_with* **<Args>** *on_method* **<Name>** *and_return_value* **<Value>**
    """

    def __init__(self, method_name=None, builder=None):
        self._method_name = method_name
        self._builder = builder or SideEffectBuilder()

    def _set_method_name(self, method_name: str) -> "SideEffectBuilderGrammar":
        if self._method_name:
            raise AllowsException("Cannot set mutliple method names in one effect")
        self._method_name = method_name
        return self

    def on_method(self, method_name):
        """
        Specify method name on the mocked object which will have the side effect applied.

        allow(my_mock).to(return_value(5).on_method('foo'))
        assert my_mock.foo() == 5
        """
        self._set_method_name(method_name)
        return self

    def called_with(self, *args, **kwargs):
        """
        Specify call args that trigger the side effect. Alias ``when_called_with``
        """
        self._builder.with_call_args(*args, **kwargs)
        return self

    def when_called_with(self, *args, **kwargs):
        self._builder.with_call_args(*args, **kwargs)
        return self

    def and_return(self, *return_value):
        self._builder.with_return_value(*return_value)
        return self

    def and_return_value(self, *return_value):
        """
        Add a return value (or a list of return values to cycle through). Alias ``and_return``
        """
        self._builder.with_return_value(*return_value)
        return self

    def and_raise(self, raised_exception):
        self._builder.with_raised_exception(raised_exception)
        return self

    def and_raise_exception(self, raised_exception):
        """
        Raise an exception. Alias ``and_raise``
        """
        self._builder.with_raised_exception(raised_exception)
        return self

    def with_effect(self, effect: Callable):
        """
        Apply a generic effect when invoking the side effect.
        """
        self._builder.with_effect(effect)
        return self

    def apply_to(self, mock_subject: Mock) -> Mock:
        """
        Apply the built side effect to the given Python Mock.
        """
        mock_method = self._get_method_from_mock(mock_subject)
        side_effect = self._builder.build()
        mock_method.side_effect = side_effect.merge(mock_method.side_effect)
        return mock_subject

    def _get_method_from_mock(self, mock_subject):
        if not self._method_name:
            return mock_subject
        method_path = self._method_name.split(".")
        for method in method_path:
            mock_subject = getattr(mock_subject, method)
        return mock_subject