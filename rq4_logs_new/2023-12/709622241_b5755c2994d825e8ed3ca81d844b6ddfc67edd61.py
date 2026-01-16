from __future__ import annotations

__all__ = ["DecimalToNumericPreprocessor"]


import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from flamme.preprocessor.base import BasePreprocessor
from flamme.utils.columns import find_columns_decimal


class DecimalToNumericPreprocessor(BasePreprocessor):
    r"""Implements a preprocessor to convert all the columns with
    ``Decimal`` objects to floats.

    Args:
    ----
        **kwargs: Specifies the keyword arguments for
            ``pandas.to_numeric``.

    Example usage:

    .. code-block:: pycon

        >>> from decimal import Decimal
        >>> import pandas as pd
        >>> from flamme.preprocessor import DecimalToNumericPreprocessor
        >>> preprocessor = DecimalToNumericPreprocessor()
        >>> preprocessor
        DecimalToNumericPreprocessor()
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": [1, 2, 3, 4, Decimal(5)],
        ...         "col2": [Decimal(1), Decimal(2), Decimal(3), Decimal(4), Decimal(5)],
        ...         "col3": ["1", "2", "3", "4", "5"],
        ...         "col4": ["a", "b", "c", "d", "e"],
        ...     }
        ... )
        >>> df.dtypes
        col1    object
        col2    object
        col3    object
        col4    object
        dtype: object
        >>> df = preprocessor.preprocess(df)
        >>> df.dtypes
        col1     int64
        col2   float64
        col3    object
        col4    object
        dtype: object
    """

    def __init__(self, **kwargs) -> None:
        self._kwargs = kwargs

    def __repr__(self) -> str:
        args = ", ".join([f"{key}={value}" for key, value in self._kwargs.items()])
        return f"{self.__class__.__qualname__}({args})"

    def preprocess(self, df: DataFrame) -> DataFrame:
        columns = find_columns_decimal(df)
        for col in tqdm(columns, desc="Converting Decimal to numeric type"):
            df[col] = pd.to_numeric(df[col], **self._kwargs)
        return df