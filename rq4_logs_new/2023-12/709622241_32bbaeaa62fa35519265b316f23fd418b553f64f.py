from __future__ import annotations

__all__ = ["ChoiceAnalyzer"]

from collections.abc import Callable, Mapping

from coola.utils import str_indent, str_mapping
from pandas import DataFrame

from flamme.analyzer.base import BaseAnalyzer, setup_analyzer
from flamme.section import BaseSection


class ChoiceAnalyzer(BaseAnalyzer):
    r"""Implements an analyzer to analyze multiple analyzers.

    Args:
        analyzers (``Mapping``): Specifies the mappings to analyze.
            The key of each analyzer is used to organize the metrics
            and report.
        selection_fn: Specifies a callable with the selection logic.
            The callable returns the key of the analyzer to use based
            on the data in the input DataFrame.

    Example usage:

    ```pycon
    >>> import numpy as np
    >>> import pandas as pd
    >>> from flamme.analyzer import (
    ...     ChoiceAnalyzer,
    ...     FilteredAnalyzer,
    ...     NullValueAnalyzer,
    ...     DuplicatedRowAnalyzer,
    ... )
    >>> analyzer = ChoiceAnalyzer(
    ...     {"null": NullValueAnalyzer(), "duplicate": DuplicatedRowAnalyzer()},
    ...     selection_fn=lambda df: "null" if df.isnull().values.any() else "duplicate",
    ... )
    >>> analyzer
    ChoiceAnalyzer(
      (null): NullValueAnalyzer(figsize=None)
      (duplicate): DuplicatedRowAnalyzer(columns=None, figsize=None)
    )
    >>> df = pd.DataFrame(
    ...     {
    ...         "int": np.array([np.nan, 1, 0, 1]),
    ...         "float": np.array([1.2, 4.2, np.nan, 2.2]),
    ...         "str": np.array(["A", "B", None, np.nan]),
    ...     }
    ... )
    >>> section = analyzer.analyze(df)
    >>> section.__class__.__qualname__
    NullValueSection
    >>> df = pd.DataFrame({"col": np.arange(10)})
    >>> section = analyzer.analyze(df)
    >>> section.__class__.__qualname__
    DuplicatedRowSection

    ```
    """

    def __init__(
        self, analyzers: Mapping[str, BaseAnalyzer | dict], selection_fn: Callable[[DataFrame], str]
    ) -> None:
        self._analyzers = {name: setup_analyzer(analyzer) for name, analyzer in analyzers.items()}
        self._selection_fn = selection_fn

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(\n  {str_indent(str_mapping(self._analyzers))}\n)"

    @property
    def analyzers(self) -> dict[str, BaseAnalyzer]:
        return self._analyzers

    def analyze(self, df: DataFrame) -> BaseSection:
        analyzer = self._analyzers[self._selection_fn(df)]
        return analyzer.analyze(df)