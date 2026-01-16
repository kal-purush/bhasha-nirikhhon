from __future__ import annotations

__all__ = ["ToDatetimeDataFrameTransformer"]

from collections.abc import Sequence

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from flamme.transformer.df.base import BaseDataFrameTransformer


class ToDatetimeDataFrameTransformer(BaseDataFrameTransformer):
    r"""Implements a transformer to convert some columns to numeric type.

    Args:
    ----
        columns (``Sequence``): Specifies the columns to convert.
        **kwargs: Specifies the keyword arguments for
            ``pandas.to_datetime``.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.df import ToDatetime
        >>> transformer = ToDatetime(columns=["col1"])
        >>> transformer
        ToDatetimeDataFrameTransformer(columns=('col1',))
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": ["2020-1-1", "2020-1-2", "2020-1-31", "2020-12-31", "2021-12-31"],
        ...         "col2": [1, 2, 3, 4, 5],
        ...         "col3": ["a", "b", "c", "d", "e"],
        ...     }
        ... )
        >>> df.dtypes
        col1    object
        col2     int64
        col3    object
        dtype: object
        >>> df = transformer.transform(df)
        >>> df.dtypes
        col1    datetime64[ns]
        col2             int64
        col3            object
        dtype: object
    """

    def __init__(self, columns: Sequence[str], **kwargs) -> None:
        self._columns = tuple(columns)
        self._kwargs = kwargs

    def __repr__(self) -> str:
        args = ", ".join([f"{key}={value}" for key, value in self._kwargs.items()])
        if args:
            args = ", " + args
        return f"{self.__class__.__qualname__}(columns={self._columns}{args})"

    def transform(self, df: DataFrame) -> DataFrame:
        for col in tqdm(self._columns, desc="Converting to numeric type"):
            df[col] = pd.to_datetime(df[col], **self._kwargs)
        return df