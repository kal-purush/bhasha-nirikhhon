from __future__ import annotations

__all__ = ["ToNumericSeriesTransformer"]


import pandas as pd
from pandas import Series

from flamme.transformer.series.base import BaseSeriesTransformer


class ToNumericSeriesTransformer(BaseSeriesTransformer):
    r"""Implements a ``pandas.Series`` transformer to convert a
    ``pandas.Series`` to numeric type.

    Args:
    ----
        **kwargs: Specifies the keyword arguments for
            ``pandas.to_numeric``.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.series import ToNumeric
        >>> transformer = ToNumeric()
        >>> transformer
        ToNumericSeriesTransformer()
        >>> series = pd.Series(["1", "2", "3", "4", "5"])
        >>> series.dtype
        dtype('O')
        >>> series = transformer.preprocess(series)
        >>> series.dtype
        dtype('int64')
    """

    def __init__(self, **kwargs) -> None:
        self._kwargs = kwargs

    def __repr__(self) -> str:
        args = ", ".join([f"{key}={value}" for key, value in self._kwargs.items()])
        return f"{self.__class__.__qualname__}({args})"

    def preprocess(self, series: Series) -> Series:
        return pd.to_numeric(series, **self._kwargs)