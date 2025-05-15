from summer_modules.charts import plot_bar_chart
from tests.test_main import SUMMER_MODULES_TEST_LOGGER
from pathlib import Path

CURRENT_DIR = Path(__file__).parent.resolve()


def test_plot_bar_chart():
    data = [1, 2, 3, 4, 5]
    plot_bar_chart(
        data=data,
        title="测试列表参数柱形图",
        xlabel="X-axis",
        ylabel="Y-axis",
        save_path=CURRENT_DIR / "test_bar_chart.png",
    )
    data_dict = {"A": 1, "B": 2, "C": 3}
    plot_bar_chart(
        data=data_dict,
        title="测试字典参数柱形图",
        xlabel="X-axis",
        ylabel="Y-axis",
        save_path=CURRENT_DIR / "test_bar_chart_dict.png",
    )


if __name__ == "__main__":
    SUMMER_MODULES_TEST_LOGGER.info("开始测试")
    test_plot_bar_chart()
    SUMMER_MODULES_TEST_LOGGER.info("测试完成")
