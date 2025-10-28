"""Simple example to generate bar charts."""

from pathlib import Path

from summer_modules_charts import plot_bar_chart

from . import EXAMPLES_ROOT


def main() -> None:
    output_dir = EXAMPLES_ROOT / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_bar_chart(
        data=[1, 2, 3, 4, 5],
        title="列表数据柱状图",
        xlabel="X-axis",
        ylabel="Y-axis",
        save_path=output_dir / "list_bar_chart.png",
    )
    plot_bar_chart(
        data={"A": 1, "B": 2, "C": 3},
        title="字典数据柱状图",
        xlabel="X-axis",
        ylabel="Y-axis",
        save_path=output_dir / "dict_bar_chart.png",
    )


if __name__ == "__main__":
    main()
