from __future__ import annotations

__all__ = ["ToDatetimeSeriesTransformer"]


import pandas as pd
from pandas import Series

from flamme.transformer.series.base import BaseSeriesTransformer


class ToDatetimeSeriesTransformer(BaseSeriesTransformer):
    r"""Implements a preprocessor to convert some columns to datetime
    type.

    Args:
    ----
        **kwargs: Specifies the keyword arguments for
            ``pandas.to_datetime``.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.series import ToDatetime
        >>> transformer = ToDatetime()
        >>> transformer
        ToDatetimeSeriesTransformer()
        >>> series = pd.Series(["2020-1-1", "2020-1-2", "2020-1-31", "2020-12-31", "2021-12-31"])
        >>> series.dtype
        dtype('O')
        >>> series = transformer.preprocess(series)
        >>> series.dtype
        dtype('<M8[ns]')
    """

    def __init__(self, **kwargs) -> None:
        self._kwargs = kwargs

    def __repr__(self) -> str:
        args = ", ".join([f"{key}={value}" for key, value in self._kwargs.items()])
        return f"{self.__class__.__qualname__}({args})"

    def preprocess(self, series: Series) -> Series:
        return pd.to_datetime(series, **self._kwargs)