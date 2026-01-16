from __future__ import annotations

__all__ = ["figure2html"]

import base64
import io

from matplotlib import pyplot as plt


def figure2html(fig: plt.Figure) -> str:
    r"""Converts a matplotlib figure to a string that can be used in a
    HTML file.

    Args:
    ----
        fig (``Figure``): Specifies the figure to convert.

    Returns:
    -------
        str: The converted figure to a string.
    """
    img = io.BytesIO()
    fig.savefig(img, format="png", bbox_inches="tight")
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode("utf-8")