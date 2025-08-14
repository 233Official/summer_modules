from summer_modules.database.postgres.table_partitions_manage import (
    parse_partition_suffix,
    parse_partition_table_year_month,
)
from tests import SUMMER_MODULES_TEST_LOGGER


def test_parse_partition_suffix():
    suffix_schema = "y2025m08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")
    formatted = template.format(year=2025, month="08")
    SUMMER_MODULES_TEST_LOGGER.info(f"格式化后的字符串: {formatted}")

    suffix_schema = "202508"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")
    formatted = template.format(year=2025, month="08")
    SUMMER_MODULES_TEST_LOGGER.info(f"格式化后的字符串: {formatted}")

    suffix_schema = "2025m08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")

    suffix_schema = "2025m08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")

    suffix_schema = "2025_08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")

    suffix_schema = "2025-08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")

    suffix_schema = "2025.08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")

    suffix_schema = "2025/08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")

    suffix_schema = "2025\\08"
    template = parse_partition_suffix(suffix_schema)
    SUMMER_MODULES_TEST_LOGGER.info(f"解析{suffix_schema}得到模板{template}")


def test_parse_partition_table_year_month():
    table_name = "test_table_y2025m08"
    suffix_template = parse_partition_suffix("y2025m08")
    year, month = parse_partition_table_year_month(table_name, suffix_template)
    SUMMER_MODULES_TEST_LOGGER.info(f"从{table_name}提取到年份: {year}, 月份: {month}")

    table_name = "test_table_202508"
    suffix_template = parse_partition_suffix("202508")
    year, month = parse_partition_table_year_month(table_name, suffix_template)
    SUMMER_MODULES_TEST_LOGGER.info(f"从{table_name}提取到年份: {year}, 月份: {month}")

def main():
    # test_parse_partition_suffix()
    test_parse_partition_table_year_month()


if __name__ == "__main__":
    main()
