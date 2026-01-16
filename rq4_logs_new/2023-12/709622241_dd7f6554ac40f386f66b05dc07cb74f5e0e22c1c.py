from __future__ import annotations

__all__ = ["ColumnSelectionDataFrameTransformer"]

from collections.abc import Sequence

from pandas import DataFrame

from flamme.transformer.df.base import BaseDataFrameTransformer


class ColumnSelectionDataFrameTransformer(BaseDataFrameTransformer):
    r"""Implements a ``pandas.DataFrame`` transformer to select a subset
    of columns.

    Args:
    ----
        columns (``Sequence``): Specifies the columns to keep.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.df import ColumnSelection
        >>> transformer = ColumnSelection(columns=["col1", "col2"])
        >>> transformer
        ColumnSelectionDataFrameTransformer(columns=['col1', 'col2'])
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": ["2020-1-1", "2020-1-2", "2020-1-31", "2020-12-31", None],
        ...         "col2": [1, 2, 3, 4, 5],
        ...         "col3": ["a", "b", "c", "d", "e"],
        ...     }
        ... )
        >>> df = transformer.transform(df)
        >>> df
                 col1  col2
        0    2020-1-1     1
        1    2020-1-2     2
        2   2020-1-31     3
        3  2020-12-31     4
        4        None     5
    """

    def __init__(self, columns: Sequence[str]) -> None:
        self._columns = list(columns)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(columns={self._columns})"

    def transform(self, df: DataFrame) -> DataFrame:
        for col in self._columns:
            if col not in df:
                raise RuntimeError(
                    f"Column {col} is not in the DataFrame (columns:{sorted(df.columns)})"
                )
        return df[self._columns].copy()