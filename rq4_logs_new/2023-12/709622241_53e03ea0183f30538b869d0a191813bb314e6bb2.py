from __future__ import annotations

__all__ = ["ColumnTemporalNullValueSection"]

from collections.abc import Sequence

from jinja2 import Template
from pandas import DataFrame

from flamme.section.base import BaseSection
from flamme.section.utils import (
    GO_TO_TOP,
    render_html_toc,
    tags2id,
    tags2title,
    valid_h_tag,
)


class ColumnTemporalNullValueSection(BaseSection):
    r"""Implements a section to analyze the temporal distribution of null
    values for a given column.

    Args:
    ----
        df (``pandas.DataFrame``): Specifies the DataFrame to analyze.
        column (str): Specifies the column to analyze.
        dt_column (str): Specifies the datetime column used to analyze
            the temporal distribution.
        period (str): Specifies the temporal period e.g. monthly or
            daily.
        ncols (int, optional): Specifies the number of columns.
            Default: ``2``
        figsize (``tuple``, optional): Specifies the individual figure
            size in pixels. The first dimension is the width and the
            second is the height.  Default: ``(700, 300)``
    """

    def __init__(
        self,
        df: DataFrame,
        column: str,
        dt_column: str,
        period: str,
        figsize: tuple[int, int] | None = None,
    ) -> None:
        if column not in df:
            raise ValueError(
                f"Column {column} is not in the DataFrame (columns:{sorted(df.columns)})"
            )
        if dt_column not in df:
            raise ValueError(
                f"Datetime column {dt_column} is not in the DataFrame (columns:{sorted(df.columns)})"
            )

        self._df = df
        self._column = column
        self._dt_column = dt_column
        self._period = period
        self._figsize = figsize

    @property
    def df(self) -> DataFrame:
        r"""``pandas.DataFrame``: The DataFrame to analyze."""
        return self._df

    @property
    def column(self) -> str:
        r"""str: The column to analyze."""
        return self._column

    @property
    def dt_column(self) -> str:
        r"""str: The datetime column."""
        return self._dt_column

    @property
    def period(self) -> str:
        r"""str: The temporal period used to analyze the data."""
        return self._period

    @property
    def figsize(self) -> tuple[int, int]:
        r"""tuple: The individual figure size in pixels. The first
        dimension is the width and the second is the height."""
        return self._figsize

    def get_statistics(self) -> dict:
        return {}

    def render_html_body(self, number: str = "", tags: Sequence[str] = (), depth: int = 0) -> str:
        return Template(self._create_template()).render(
            {
                "go_to_top": GO_TO_TOP,
                "id": tags2id(tags),
                "depth": valid_h_tag(depth + 1),
                "title": tags2title(tags),
                "section": number,
                "column": self._dt_column,
                "figure": "",
                "table": "",
            }
        )

    def render_html_toc(
        self, number: str = "", tags: Sequence[str] = (), depth: int = 0, max_depth: int = 1
    ) -> str:
        return render_html_toc(number=number, tags=tags, depth=depth, max_depth=max_depth)

    def _create_template(self) -> str:
        return """
<h{{depth}} id="{{id}}">{{section}} {{title}} </h{{depth}}>

{{go_to_top}}

<p style="margin-top: 1rem;">
This section analyzes the monthly distribution of null values.
The column {{column}} is used to define the month of each row.

{{figure}}
"""