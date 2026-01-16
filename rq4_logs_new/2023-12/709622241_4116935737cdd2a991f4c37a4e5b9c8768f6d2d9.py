from __future__ import annotations

__all__ = ["DuplicateRowAnalyzer"]

import logging
from collections.abc import Sequence

from pandas import DataFrame

from flamme.analyzer.base import BaseAnalyzer
from flamme.section import DuplicatedRowSection

logger = logging.getLogger(__name__)


class DuplicateRowAnalyzer(BaseAnalyzer):
    r"""Implements an analyzer to show the number of duplicated rows.

    Args:
    ----
        columns (``Sequence`` or ``None``): Specifies the columns used
            to compute the duplicated rows. ``None`` means all the
            columns. Default: ``None``

    Example usage:

    .. code-block:: pycon

        >>> import numpy as np
        >>> import pandas as pd
        >>> from flamme.analyzer import DuplicateRowAnalyzer
        >>> analyzer = DuplicateRowAnalyzer()
        >>> analyzer
        DuplicateRowAnalyzer(columns=None)
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": np.array([0, 1, 0, 1]),
        ...         "col2": np.array([1, 0, 1, 0]),
        ...         "col3": np.array(
        ...             [
        ...                 1,
        ...                 1,
        ...                 1,
        ...                 1,
        ...             ]
        ...         ),
        ...     }
        ... )
        >>> section = analyzer.analyze(df)
    """

    def __init__(self, columns: Sequence[str] | None = None) -> None:
        self._columns = columns

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(columns={self._columns})"

    def analyze(self, df: DataFrame) -> DuplicatedRowSection:
        logger.info(f"Analyzing the duplicated rows section using the columns: {self._columns}")
        return DuplicatedRowSection(df=df, columns=self._columns)