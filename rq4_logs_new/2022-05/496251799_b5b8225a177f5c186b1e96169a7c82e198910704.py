import enum
from functools import cached_property
import typing

from ..squid import SquidProvider

if typing.TYPE_CHECKING:
    from . import DataProvider


@enum.unique
class Provider(str, enum.Enum):
    squid = 'squid'

    @cached_property
    def _object_classes(self):
        return {
            Provider.squid.name: SquidProvider
        }

    def get_object(self) -> 'DataProvider':
        return self._object_classes[self.name]()