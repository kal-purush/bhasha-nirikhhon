from __future__ import annotations

__all__ = ["StripStrDataFrameTransformer"]

from collections.abc import Sequence

from pandas import DataFrame
from tqdm import tqdm

from flamme.transformer.df.base import BaseDataFrameTransformer


class StripStrDataFrameTransformer(BaseDataFrameTransformer):
    r"""Implements a transformer to strip the strings of some columns.

    Args:
    ----
        columns (``Sequence``): Specifies the columns to process.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.df import StripStrDataFrameTransformer
        >>> transformer = StripStrDataFrameTransformer(columns=["col1", "col3"])
        >>> transformer
        StripStrDataFrameTransformer(columns=('col1', 'col3'))
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": [1, 2, 3, 4, 5],
        ...         "col2": ["1", "2", "3", "4", "5"],
        ...         "col3": ["a ", " b", "  c  ", "d", "e"],
        ...         "col4": ["a ", " b", "  c  ", "d", "e"],
        ...     }
        ... )
        >>> df
           col1 col2   col3   col4
        0     1    1     a      a
        1     2    2      b      b
        2     3    3    c      c
        3     4    4      d      d
        4     5    5      e      e
        >>> df = transformer.transform(df)
        >>> df
           col1 col2 col3   col4
        0     1    1    a     a
        1     2    2    b      b
        2     3    3    c    c
        3     4    4    d      d
        4     5    5    e      e
    """

    def __init__(self, columns: Sequence[str]) -> None:
        self._columns = tuple(columns)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(columns={self._columns})"

    def transform(self, df: DataFrame) -> DataFrame:
        for col in tqdm(self._columns, desc="Striping strings"):
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        return df