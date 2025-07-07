from pathlib import Path

from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG

from summer_modules.database.hbase.ssh_output_resolve import (
    parse_manual_full_export_file_to_json,
)


def test_parse_manual_full_export_file_to_json():
    src_filepath = CONFIG["hbase_ssh_output_test"]["src_filepath"]
    # 不分片
    parse_manual_full_export_file_to_json(src_filepath=Path(src_filepath))
    # 分片(developing......)
    # parse_manual_full_export_file_to_json(
    #     src_filepath=Path(src_filepath), data_size_limit=50 * 1024 * 1024
    # )  # 500MB


def main():
    test_parse_manual_full_export_file_to_json()


if __name__ == "__main__":
    main()
