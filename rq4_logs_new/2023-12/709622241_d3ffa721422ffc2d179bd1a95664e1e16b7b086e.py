from __future__ import annotations

__all__ = ["StripStringSeriesTransformer"]


from pandas import Series

from flamme.transformer.series.base import BaseSeriesTransformer


class StripStringSeriesTransformer(BaseSeriesTransformer):
    r"""Implements a transformer to strip the strings in a
    ``pandas.Series``.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.series import StripString
        >>> transformer = StripString()
        >>> transformer
        StripStringSeriesTransformer()
        >>> series = pd.Series(["a ", " b", "  c  ", " d ", "e"])
        >>> series = transformer.transform(series)
        >>> series
        0    a
        1    b
        2    c
        3    d
        4    e
        dtype: object
    """

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}()"

    def transform(self, series: Series) -> Series:
        return series.apply(lambda x: x.strip() if isinstance(x, str) else x)