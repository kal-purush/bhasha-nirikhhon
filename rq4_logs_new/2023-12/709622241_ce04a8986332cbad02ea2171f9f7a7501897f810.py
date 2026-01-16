from __future__ import annotations

__all__ = [
    "BaseColumn",
    "Column",
    "StringColumn",
    "NumericColumn",
    # "NumericDiscreteColumn",
    # "NumericContinuousColumn",
]

from abc import abstractmethod

from coola.utils import str_indent, str_mapping

from flamme.transformer.series.base import (
    BaseSeriesTransformer,
    setup_series_transformer,
)
from flamme.transformer.series.numeric import ToNumericSeriesTransformer
from flamme.transformer.series.string import StripStringSeriesTransformer


class BaseColumn:
    r"""Defines the column base class."""

    @abstractmethod
    def get_transformer(self) -> BaseSeriesTransformer:
        r"""Gets the column transformer.

        Returns:
        -------
            ``BaseSeriesTransformer``: The column transformer.
        """


class Column(BaseColumn):
    r"""Defines the column base class.

    Args:
    ----
        can_be_null (bool): ``True`` if the column can have null
            values, otherwise ``False``.
    """

    def __init__(self, can_be_null: bool, transformer: BaseSeriesTransformer | dict) -> None:
        self._can_be_null = bool(can_be_null)
        self._transformer = setup_series_transformer(transformer)

    def __repr__(self) -> str:
        args = str_indent(
            str_mapping({"can_be_null": self._can_be_null, "transformer": self._transformer})
        )
        return f"{self.__class__.__qualname__}({args})"

    @property
    def can_be_null(self) -> bool:
        r"""bool: ``True`` if the column can have null values, otherwise ``False``"""
        return self._can_be_null

    def get_transformer(self) -> BaseSeriesTransformer:
        return self._transformer


class NumericColumn(Column):
    r"""Defines a column with numeric values.

    Args:
    ----
        can_be_null (bool): ``True`` if the column can have null
            values, otherwise ``False``.
        transformer (``BaseSeriesTransformer`` dict or ``None``,
            optional): Specifies the transformer or its configuration.
            Default: ``None``
    """

    def __init__(
        self,
        can_be_null: bool,
        transformer: BaseSeriesTransformer | dict | None = None,
    ) -> None:
        if transformer is None:
            transformer = ToNumericSeriesTransformer()
        super().__init__(can_be_null=can_be_null, transformer=transformer)


class StringColumn(Column):
    r"""Defines a column with string values.

    Args:
    ----
        can_be_null (bool): ``True`` if the column can have null
            values, otherwise ``False``.
        transformer (``BaseSeriesTransformer`` dict or ``None``,
            optional): Specifies the transformer or its configuration.
            Default: ``None``
    """

    def __init__(
        self,
        can_be_null: bool,
        transformer: BaseSeriesTransformer | dict | None = None,
    ) -> None:
        if transformer is None:
            transformer = StripStringSeriesTransformer()
        super().__init__(can_be_null=can_be_null, transformer=transformer)


# class NumericContinuousColumn(BaseColumn):
#     pass
#
#
# class NumericDiscreteColumn(BaseColumn):
#     pass