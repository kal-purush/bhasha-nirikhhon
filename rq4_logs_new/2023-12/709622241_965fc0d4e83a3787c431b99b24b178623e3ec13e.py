from __future__ import annotations

__all__ = ["sorted_nan"]

import math
from collections.abc import Sequence
from typing import Any


def sorted_nan(data: Sequence[int | float], reverse: bool = False) -> list[float]:
    r"""Implements a function to sort a sequence of numeric values with
    NaN.

    This function is an extension of the built-in ``sorted`` function.
    It sees NaN values as equivalent to -infinity when the values are
    sorted.

    Args:
        data (``Sequence``): Specifies the sequence of numeric values
            to sort.
        reverse (``bool``, optional): If set to ``True``, then the
            list elements are sorted as if each comparison were
            reversed. Default: ``False``

    Returns:
        list: The sorted list.

     Example usage:

    .. code-block:: pycon

        >>> from flamme.utils.mathnan import sorted_nan
        >>> x = [4, float("nan"), 2, 1.2, 7.9, -2]
        >>> sorted(x)
        [4, nan, -2, 1.2, 2, 7.9]
        >>> sorted_nan(x)
        [nan, -2, 1.2, 2, 4, 7.9]
        >>> sorted_nan(x, reverse=True)
        [7.9, 4, 2, 1.2, -2, nan]
    """
    return sorted(data, key=lambda x: LowNaN() if math.isnan(x) else x, reverse=reverse)


class LowNaN(float):
    r"""Implements a NaN representation that is always lower than other
    numbers.

    This class is designed to be used to compare numbers with NaN values
    and should not be used in other cases.

    https://docs.python.org/3/library/functions.html#sorted
    """

    def __ge__(self, other: Any) -> bool:
        if not isinstance(other, (float, int)):
            raise TypeError(
                f"'>=' not supported between instances of 'float' and '{type(other).__qualname__}'"
            )
        return False

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, (float, int)):
            raise TypeError(
                f"'>' not supported between instances of 'float' and '{type(other).__qualname__}'"
            )
        return False

    def __le__(self, other: Any) -> bool:
        if not isinstance(other, (float, int)):
            raise TypeError(
                f"'<=' not supported between instances of 'float' and '{type(other).__qualname__}'"
            )
        return True

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, (float, int)):
            raise TypeError(
                f"'<' not supported between instances of 'float' and '{type(other).__qualname__}'"
            )
        return True