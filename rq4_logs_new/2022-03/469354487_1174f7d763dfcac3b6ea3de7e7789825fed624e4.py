from typing import Any, Callable

import pytest

from coil.types import (
    DataDeletedEvent,
    DataUpdatedEvent,
    is_data_event,
    is_delete_event,
    is_update_event,
)

from .conftest import Box


@pytest.mark.parametrize(
    "func,obj,expected",
    [
        (is_data_event, {}, False),
        (
            is_data_event,
            DataDeletedEvent(
                source_event=None,
                source=Box.value.bind(Box(10)),
            ),
            True,
        ),
        (
            is_data_event,
            DataUpdatedEvent(
                source_event=None,
                source=Box.value.bind(Box(10)),
                value=11,
            ),
            True,
        ),
        (
            is_delete_event,
            DataDeletedEvent(
                source_event=None, source=Box.value.bind(Box(10))
            ),
            True,
        ),
        (
            is_delete_event,
            DataUpdatedEvent(
                source_event=None,
                source=Box.value.bind(Box(10)),
                value=11,
            ),
            False,
        ),
        (
            is_update_event,
            DataDeletedEvent(
                source_event=None, source=Box.value.bind(Box(10))
            ),
            False,
        ),
        (
            is_update_event,
            DataUpdatedEvent(
                source_event=None,
                source=Box.value.bind(Box(10)),
                value=11,
            ),
            True,
        ),
    ],
)
def test_predicate_functions(
    func: Callable[[Any], bool], obj: Any, expected: bool
) -> None:
    assert func(obj) == expected