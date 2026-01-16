from __future__ import annotations

__all__ = ["compute_null_per_col"]

import numpy as np
from pandas import DataFrame


def compute_null_per_col(df: DataFrame) -> DataFrame:
    r"""Computes the number and percentage of null values per column.

    Args:
    ----
        df (``pandas.DataFrame``): Specifies the DataFrame to analyze.

    Returns:
    -------
        ``pandas.DataFrame``: A DataFrame with the number and
            percentage of null values per column.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.utils.null import compute_null_per_col
        >>> df = compute_null_per_col(
        ...     pd.DataFrame(
        ...         {
        ...             "int": np.array([np.nan, 1, 0, 1]),
        ...             "float": np.array([1.2, 4.2, np.nan, 2.2]),
        ...             "str": np.array(["A", "B", None, np.nan]),
        ...         }
        ...     )
        ... )
        >>> df
          column  null  total  null_pct
        0    int     1      4      0.25
        1  float     1      4      0.25
        2    str     2      4      0.50
    """
    null_count = df.isnull().sum().to_frame("count")["count"].to_numpy().astype(int)
    total_count = np.full((df.shape[1],), df.shape[0]).astype(int)
    return DataFrame(
        {
            "column": list(df.columns),
            "null": null_count,
            "total": total_count,
            "null_pct": (null_count / total_count).astype(float),
        }
    )