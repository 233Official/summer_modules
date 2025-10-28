"""Example usage of table partition utilities."""

from summer_modules_database.postgres.table_partitions_manage import (
    parse_partition_suffix,
    parse_partition_table_year_month,
)


def main() -> None:
    suffix_schema = "y2025m08"
    template = parse_partition_suffix(suffix_schema)
    print(f"解析 {suffix_schema} 得到模板: {template}")
    print(template.format(year=2025, month="08"))

    table_name = "test_table_202508"
    template = parse_partition_suffix("202508")
    year, month = parse_partition_table_year_month(table_name, template)
    print(f"从 {table_name} 提取到年份: {year}, 月份: {month}")


if __name__ == "__main__":
    main()
