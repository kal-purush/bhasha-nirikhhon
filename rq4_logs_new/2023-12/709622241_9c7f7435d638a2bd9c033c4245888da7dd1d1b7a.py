from __future__ import annotations

__all__ = ["nonnan"]

import numpy as np


def nonnan(array: np.ndarray) -> np.ndarray:
    r"""Returns the non-NaN values of an array.

    Args:
        array: Specifies the input array.

    Returns:
        A 1d array with the non-NaN values of the input array.

    Example usage:

    ```pycon
    >>> import numpy as np
    >>> from flamme.utils.array import nonnan
    >>> nonnan(np.asarray([1, 2, float("nan"), 5, 6]))
    array([1., 2., 5., 6.])
    >>> nonnan(np.asarray([[1, 2, float("nan")], [4, 5, 6]]))
    array([1., 2., 4., 5., 6.])

    ```
    """
    mask = np.isnan(array)
    return array[~mask]