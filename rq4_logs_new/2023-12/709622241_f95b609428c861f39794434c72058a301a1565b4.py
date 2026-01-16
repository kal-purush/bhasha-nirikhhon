from __future__ import annotations

__all__ = ["DuplicatedRowSection"]

import logging
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

logger = logging.getLogger(__name__)


class DuplicatedRowSection(BaseSection):
    r"""Implements a section to analyze the number of duplicated rows.

    Args:
    ----
        df (``pandas.DataFrame``): Specifies the DataFrame to analyze.
        columns (``Sequence`` or ``None``): Specifies the columns used
            to compute the duplicated rows. ``None`` means all the
            columns. Default: ``None``
    """

    def __init__(self, df: DataFrame, columns: Sequence[str] | None = None) -> None:
        self._df = df
        self._columns = columns if columns is None else tuple(columns)

    @property
    def df(self) -> DataFrame:
        r"""``pandas.DataFrame``: The DataFrame to analyze."""
        return self._df

    @property
    def columns(self) -> tuple[str, ...] | None:
        r"""str: The columns used to compute the duplicated rows."""
        return self._columns

    def get_statistics(self) -> dict:
        df_no_duplicate = self._df.drop_duplicates(subset=self._columns)
        return {"num_rows": self._df.shape[0], "num_unique_rows": df_no_duplicate.shape[0]}

    def render_html_body(self, number: str = "", tags: Sequence[str] = (), depth: int = 0) -> str:
        logger.info(f"Rendering the duplicated rows section using the columns: {self._columns}")
        return Template(self._create_template()).render(
            {
                "go_to_top": GO_TO_TOP,
                "id": tags2id(tags),
                "depth": valid_h_tag(depth + 1),
                "title": tags2title(tags),
                "section": number,
                "columns": self._columns,
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
This section shows the number of duplicated rows using the following columns <em>{{columns}}</em>.

{{table}}
<p style="margin-top: 1rem;">
"""