from __future__ import annotations

__all__ = ["BaseSeriesTransformer", "is_series_transformer_config", "setup_series_transformer"]

import logging
from abc import ABC

from objectory import AbstractFactory
from objectory.utils import is_object_config
from pandas import Series

logger = logging.getLogger(__name__)


class BaseSeriesTransformer(ABC, metaclass=AbstractFactory):
    r"""Defines the base class to preprocess a ``pandas.Series``.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.series import ToNumeric
        >>> transformer = ToNumeric()
        >>> transformer
        ToNumericSeriesTransformer()
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": [1, 2, 3, 4, 5],
        ...         "col2": ["1", "2", "3", "4", "5"],
        ...         "col3": ["1", "2", "3", "4", "5"],
        ...         "col4": ["a", "b", "c", "d", "e"],
        ...     }
        ... )
        >>> df.dtypes
        col1     int64
        col2    object
        col3    object
        col4    object
        dtype: object
        >>> df = transformer.preprocess(df)
        >>> df.dtypes
        col1     int64
        col2    object
        col3     int64
        col4    object
        dtype: object
    """

    def preprocess(self, df: Series) -> Series:
        r"""Preprocesses the data in the Series.

        Args:
        ----
            df (``pandas.Series``): Specifies the Series
                to preprocess.

        Returns:
        -------
            ``pandas.Series``: The preprocessed Series.

        Example usage:

        .. code-block:: pycon

            >>> import pandas as pd
            >>> from flamme.transformer import ToNumeric
            >>> transformer = ToNumeric(columns=["col1", "col3"])
            >>> df = pd.DataFrame(
            ...     {
            ...         "col1": [1, 2, 3, 4, 5],
            ...         "col2": ["1", "2", "3", "4", "5"],
            ...         "col3": ["1", "2", "3", "4", "5"],
            ...         "col4": ["a", "b", "c", "d", "e"],
            ...     }
            ... )
            >>> df.dtypes
            col1     int64
            col2    object
            col3    object
            col4    object
            dtype: object
            >>> df = transformer.preprocess(df)
            >>> df.dtypes
            col1     int64
            col2    object
            col3     int64
            col4    object
            dtype: object
        """


def is_series_transformer_config(config: dict) -> bool:
    r"""Indicates if the input configuration is a configuration for a
    ``BaseSeriesTransformer``.

    This function only checks if the value of the key  ``_target_``
    is valid. It does not check the other values. If ``_target_``
    indicates a function, the returned type hint is used to check
    the class.

    Args:
    ----
        config (dict): Specifies the configuration to check.

    Returns:
    -------
        bool: ``True`` if the input configuration is a configuration
            for a ``BaseSeriesTransformer`` object.

    Example usage:

    .. code-block:: pycon

        >>> from flamme.transformer.series import is_series_transformer_config
        >>> is_series_transformer_config({"_target_": "flamme.transformer.series.ToNumeric"})
        True
    """
    return is_object_config(config, BaseSeriesTransformer)


def setup_series_transformer(
    transformer: BaseSeriesTransformer | dict,
) -> BaseSeriesTransformer:
    r"""Sets up a ``pandas.Series`` transformer.

    The transformer is instantiated from its configuration
    by using the ``BaseSeriesTransformer`` factory function.

    Args:
    ----
        transformer (``BaseSeriesTransformer`` or dict): Specifies a
            ``pandas.Series`` transformer or its configuration.

    Returns:
    -------
        ``BaseSeriesTransformer``: An instantiated transformer.

    Example usage:

    .. code-block:: pycon

        >>> from flamme.transformer.series import setup_series_transformer
        >>> transformer = setup_series_transformer(
        ...     {"_target_": "flamme.transformer.series.ToNumeric"}
        ... )
        >>> transformer
        ToNumericSeriesTransformer()
    """
    if isinstance(transformer, dict):
        logger.info("Initializing a series transformer from its configuration... ")
        transformer = BaseSeriesTransformer.factory(**transformer)
    if not isinstance(transformer, BaseSeriesTransformer):
        logger.warning(
            f"transformer is not a `BaseSeriesTransformer` (received: {type(transformer)})"
        )
    return transformer