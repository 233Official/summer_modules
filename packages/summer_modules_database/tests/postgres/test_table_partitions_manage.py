from summer_modules_database.postgres.table_partitions_manage import (
    calculate_partition_time_range,
    parse_partition_suffix,
    parse_partition_table_year_month,
)


def test_parse_partition_suffix_variants() -> None:
    assert parse_partition_suffix("y2025m08") == "y{year}m{month}"
    assert parse_partition_suffix("202508") == "{year}{month}"
    assert parse_partition_suffix("2025_08") == "{year}_{month}"


def test_parse_partition_table_year_month() -> None:
    template = parse_partition_suffix("y2025m08")
    assert parse_partition_table_year_month("table_y2025m08", template) == (2025, 8)

    template = parse_partition_suffix("202508")
    assert parse_partition_table_year_month("table_202508", template) == (2025, 8)


def test_calculate_partition_time_range() -> None:
    partition_stats = {
        "existing_partitions": ["table_y2024m10"],
        "created_partitions": [
            {"name": "table_y2024m11"},
            {"name": "table_y2025m01"},
        ],
    }
    result = calculate_partition_time_range(
        partition_stats, "table", "y2025m08"
    )
    assert result == {"earliest": "2024-10-01", "latest": "2025-01-01"}
