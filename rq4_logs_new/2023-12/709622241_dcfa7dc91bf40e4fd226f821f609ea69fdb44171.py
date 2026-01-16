from __future__ import annotations

__all__ = [
    "BaseDataFrameTransformer",
    "is_dataframe_transformer_config",
    "setup_dataframe_transformer",
]

import logging
from abc import ABC

from objectory import AbstractFactory
from objectory.utils import is_object_config
from pandas import DataFrame

logger = logging.getLogger(__name__)


class BaseDataFrameTransformer(ABC, metaclass=AbstractFactory):
    r"""Defines the base class to preprocess a ``pandas.DataFrame``.

    Example usage:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from flamme.transformer.df import ToNumeric
        >>> transformer = ToNumeric()
        >>> transformer
        ToNumericDataFrameTransformer()
        >>> df = pd.DataFrame(["1", "2", "3", "4", "5"])
        >>> df.dtype
        dtype('O')
        >>> df = transformer.preprocess(df)
        >>> df.dtype
        dtype('int64')
    """

    def preprocess(self, df: DataFrame) -> DataFrame:
        r"""Preprocesses the data in the DataFrame.

        Args:
        ----
            df (``pandas.DataFrame``): Specifies the DataFrame
                to preprocess.

        Returns:
        -------
            ``pandas.DataFrame``: The preprocessed DataFrame.

        Example usage:

        .. code-block:: pycon

            >>> import pandas as pd
            >>> from flamme.transformer.df import ToNumeric
            >>> transformer = ToNumeric()
            >>> df = pd.DataFrame(["1", "2", "3", "4", "5"])
            >>> df = transformer.preprocess(df)
            >>> df.dtype
            dtype('int64')
        """


def is_dataframe_transformer_config(config: dict) -> bool:
    r"""Indicates if the input configuration is a configuration for a
    ``BaseDataFrameTransformer``.

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
            for a ``BaseDataFrameTransformer`` object.

    Example usage:

    .. code-block:: pycon

        >>> from flamme.transformer.df import is_dataframe_transformer_config
        >>> is_dataframe_transformer_config({"_target_": "flamme.transformer.df.ToNumeric"})
        True
    """
    return is_object_config(config, BaseDataFrameTransformer)


def setup_dataframe_transformer(
    transformer: BaseDataFrameTransformer | dict,
) -> BaseDataFrameTransformer:
    r"""Sets up a ``pandas.DataFrame`` transformer.

    The transformer is instantiated from its configuration
    by using the ``BaseDataFrameTransformer`` factory function.

    Args:
    ----
        transformer (``BaseDataFrameTransformer`` or dict): Specifies a
            ``pandas.DataFrame`` transformer or its configuration.

    Returns:
    -------
        ``BaseDataFrameTransformer``: An instantiated transformer.

    Example usage:

    .. code-block:: pycon

        >>> from flamme.transformer.df import setup_dataframe_transformer
        >>> transformer = setup_dataframe_transformer(
        ...     {"_target_": "flamme.transformer.df.ToNumeric"}
        ... )
        >>> transformer
        ToNumericDataFrameTransformer()
    """
    if isinstance(transformer, dict):
        logger.info("Initializing a DataFrame transformer from its configuration... ")
        transformer = BaseDataFrameTransformer.factory(**transformer)
    if not isinstance(transformer, BaseDataFrameTransformer):
        logger.warning(
            f"transformer is not a `BaseDataFrameTransformer` (received: {type(transformer)})"
        )
    return transformer