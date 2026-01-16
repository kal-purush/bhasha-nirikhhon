from typing import Dict, List, Protocol, runtime_checkable

from ._data_event_handler import DataEventHandler


@runtime_checkable
class Bindable(Protocol):
    """An object which can notify subscribers about changes to its
    properties.

    This protocol is implemented by any class that is decorated
    with :func:`bindableclass <coil.bindableclass>` (even though
    this currently fails mypy typecheck).
    """

    @property
    def __coil_bindings__(self) -> Dict[str, List[DataEventHandler]]:
        """Return a mapping of subscribers"""