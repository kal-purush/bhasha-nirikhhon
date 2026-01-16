from __future__ import annotations

__all__ = [
    "find_columns_decimal",
    "find_columns_str",
    "find_columns_type",
]

from decimal import Decimal

from pandas import DataFrame

from flamme.utils.dtype import df_column_types


def find_columns_type(df: DataFrame, cls: type) -> tuple[str, ...]:
    r"""Finds the list of columns that contains a given type.

    Args:
    ----
        df (``pandas.DataFrame``): Specifies the DataFrame.
        cls: Specifies the type to find.

    Returns:
    -------
        tuple: The tuple of columns with the given type.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.utils.filtering import find_columns_type
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": [1, 2, 3, 4, 5],
        ...         "col2": ["1", "2", "3", "4", "5"],
        ...         "col3": ["1", "2", "3", "4", "5"],
        ...         "col4": ["a", "b", "c", "d", "e"],
        ...     }
        ... )
        >>> find_columns_type(df, str)
        ('col2', 'col3', 'col4')
    """
    types = df_column_types(df)
    return tuple(col for col, tps in types.items() if cls in tps)


def find_columns_decimal(df: DataFrame) -> tuple[str, ...]:
    r"""Finds the list of columns that contains the type string.

    Args:
    ----
        df (``pandas.DataFrame``): Specifies the DataFrame.

    Returns:
    -------
        tuple: The tuple of columns with the type string.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from decimal import Decimal
        >>> from flamme.utils.filtering import find_columns_decimal
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": [1, 2, 3, 4, Decimal(5)],
        ...         "col2": ["1", "2", "3", "4", "5"],
        ...         "col3": ["1", "2", "3", "4", "5"],
        ...         "col4": ["a", "b", "c", "d", "e"],
        ...     }
        ... )
        >>> find_columns_decimal(df)
        ('col1',)
    """
    return find_columns_type(df, Decimal)


def find_columns_str(df: DataFrame) -> tuple[str, ...]:
    r"""Finds the list of columns that contains the type string.

    Args:
    ----
        df (``pandas.DataFrame``): Specifies the DataFrame.

    Returns:
    -------
        tuple: The tuple of columns with the type string.

    Example usage:

    .. code-block:: pycon

        >>> import numpy as np
        >>> import pandas as pd
        >>> from flamme.utils.filtering import find_columns_str
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": [1, 2, 3, 4, 5],
        ...         "col2": ["1", "2", "3", "4", "5"],
        ...         "col3": ["1", "2", "3", "4", "5"],
        ...         "col4": ["a", "b", "c", "d", "e"],
        ...     }
        ... )
        >>> find_columns_str(df)
        ('col2', 'col3', 'col4')
    """
    return find_columns_type(df, str)