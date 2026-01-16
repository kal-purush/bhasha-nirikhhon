from __future__ import annotations

from pathlib import Path

import numpy as np
import plotly
import plotly.express as px
from matplotlib import pyplot as plt

from flamme.utils.figure import figure2html
from flamme.utils.io import save_text
from flamme.utils.path import human_file_size


def compute_size(string: str) -> int:
    return len(string)


def matplotlib_histogram(data: np.ndarray, bins: int = 100) -> str:
    fig, ax = plt.subplots(figsize=(16, 8))
    ax.hist(data, bins=bins, alpha=0.4)
    return figure2html(fig)


def plotly_histogram(data: np.ndarray, bins: int = 100) -> str:
    fig = px.histogram(x=data, nbins=bins)
    return plotly.io.to_html(fig, full_html=False)


def main() -> None:
    lines = []
    for size in [1000, 10000, 100000, 1000000]:
        data = np.random.randn(size)
        figm = matplotlib_histogram(data)
        figp = plotly_histogram(data)
        print(
            f"data={data.shape[0]:,}",
            f"matplotlib={compute_size(figm):,}",
            f"plotly={compute_size(figp):,}",
        )
        lines.append(f"<h2>size={size:,}</h2>")
        lines.append(figm)
        lines.append(figp)

    path = Path.cwd().joinpath("tmp/figures.html")
    save_text("\n".join(lines), path)
    print(human_file_size(path))


if __name__ == "__main__":
    main()