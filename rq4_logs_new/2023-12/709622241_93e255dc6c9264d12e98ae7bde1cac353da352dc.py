from __future__ import annotations

__all__ = ["NullColumnPreprocessor"]


from pandas import DataFrame

from flamme.preprocessor.base import BasePreprocessor
from flamme.utils.null import compute_null_per_col


class NullColumnPreprocessor(BasePreprocessor):
    r"""Implements a preprocessor to remove the columns that have too
    many null values.

    Args:
    ----
        threshold (float): Specifies the maximum percentage of null
            values to keep columns. If the proportion of null vallues
            is greater or equal to this threshold value, the column
            is removed. If set to ``1.0``, it removes all the columns
            that have only null values.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.preprocessor import ToDatetimePreprocessor
        >>> preprocessor = NullColumnPreprocessor(threshold=1.0)
        >>> preprocessor
        NullColumnPreprocessor(threshold=1.0)
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": ["2020-1-1", "2020-1-2", "2020-1-31", "2020-12-31", None],
        ...         "col2": [1, None, 3, None, 5],
        ...         "col3": [None, None, None, None, None],
        ...     }
        ... )
        >>> df = preprocessor.preprocess(df)
        >>> df
                 col1  col2
        0    2020-1-1   1.0
        1    2020-1-2   NaN
        2   2020-1-31   3.0
        3  2020-12-31   NaN
        4        None   5.0
    """

    def __init__(self, threshold: float) -> None:
        self._threshold = float(threshold)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(threshold={self._threshold})"

    def preprocess(self, df: DataFrame) -> DataFrame:
        if df.shape[0] == 0:
            return df
        df_null = compute_null_per_col(df)
        columns = df_null[df_null["null_pct"] < self._threshold]["column"].tolist()
        return df[columns]