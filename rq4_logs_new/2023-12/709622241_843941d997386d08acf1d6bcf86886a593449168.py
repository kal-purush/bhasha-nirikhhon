from __future__ import annotations

__all__ = ["MostFrequentValuesAnalyzer"]

import logging
from collections import Counter

from pandas import DataFrame

from flamme.analyzer.base import BaseAnalyzer
from flamme.section import EmptySection, MostFrequentValuesSection

logger = logging.getLogger(__name__)


class MostFrequentValuesAnalyzer(BaseAnalyzer):
    r"""Implements a discrete distribution analyzer.

    Args:
        column: Specifies the column to analyze.
        dropna: If ``True``, the NaN values are not included in the
            analysis.
        top: Specifies the maximum number of values to show.

    Example usage:

    .. code-block:: pycon

        >>> import numpy as np
        >>> import pandas as pd
        >>> from flamme.analyzer import MostFrequentValuesAnalyzer
        >>> analyzer = MostFrequentValuesAnalyzer(column="str")
        >>> analyzer
        MostFrequentValuesAnalyzer(column=str, dropna=False, top=100)
        >>> df = pd.DataFrame({"col": np.array([np.nan, 1, 0, 1])})
        >>> section = analyzer.analyze(df)
    """

    def __init__(self, column: str, dropna: bool = False, top: int = 100) -> None:
        self._column = column
        self._dropna = bool(dropna)
        self._top = top

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__qualname__}(column={self._column}, "
            f"dropna={self._dropna}, top={self._top:,})"
        )

    def analyze(self, df: DataFrame) -> MostFrequentValuesSection | EmptySection:
        logger.info(f"Analyzing the most frequent values of {self._column}")
        if self._column not in df:
            logger.info(
                f"Skipping most frequent values analysis of column {self._column} "
                f"because it is not in the DataFrame: {sorted(df.columns)}"
            )
            return EmptySection()
        return MostFrequentValuesSection(
            counter=Counter(df[self._column].value_counts(dropna=self._dropna).to_dict()),
            column=self._column,
            top=self._top,
        )