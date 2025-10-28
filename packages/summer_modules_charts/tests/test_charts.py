from pathlib import Path

import matplotlib.pyplot as plt

from summer_modules_charts import plot_bar_chart, plot_pie_chart


def test_plot_bar_chart(tmp_path: Path) -> None:
    save_path = tmp_path / "bar.png"
    plot_bar_chart(
        data={"A": 1, "B": 2},
        title="Bar",
        xlabel="X",
        ylabel="Y",
        save_path=save_path,
    )

    assert save_path.exists()
    plt.gcf().clear()


def test_plot_pie_chart(tmp_path: Path) -> None:
    save_path = tmp_path / "pie.png"
    plot_pie_chart(data=[1, 2, 3], title="Pie", save_path=save_path)

    assert save_path.exists()
    plt.gcf().clear()
