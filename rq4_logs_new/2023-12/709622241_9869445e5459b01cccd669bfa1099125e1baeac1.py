from __future__ import annotations

__all__ = ["SequentialSeriesTransformer"]

from collections.abc import Sequence

from coola.utils import str_indent, str_sequence
from pandas import Series

from flamme.transformer.series import BaseSeriesTransformer, setup_series_transformer


class SequentialSeriesTransformer(BaseSeriesTransformer):
    r"""Implements a ``pandas.Series`` transformer to apply sequentially
    several transformers.

    Args:
    ----
        transformers (``Sequence``): Specifies the transformers or
            their configurations.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.series import Sequential, StripString, ToNumeric
        >>> transformer = Sequential([StripString(), ToNumeric()])
        >>> transformer
        SequentialSeriesTransformer(
          (0): StripStringSeriesTransformer()
          (1): ToNumericSeriesTransformer()
        )
        >>> series = pd.Series([" 1", "2 ", " 3 ", "4", "5"])
        >>> series = transformer.transform(series)
        >>> series
        0    1
        1    2
        2    3
        3    4
        4    5
        dtype: int64
    """

    def __init__(self, transformers: Sequence[BaseSeriesTransformer | dict]) -> None:
        self._transformers = tuple(
            setup_series_transformer(transformer) for transformer in transformers
        )

    def __repr__(self) -> str:
        args = ""
        if self._transformers:
            args = f"\n  {str_indent(str_sequence(self._transformers))}\n"
        return f"{self.__class__.__qualname__}({args})"

    def transform(self, series: Series) -> Series:
        for transformer in self._transformers:
            series = transformer.transform(series)
        return series