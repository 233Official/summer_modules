"""Example for parsing HBase shell output."""

from pathlib import Path

from summer_modules_database.hbase.ssh_output_resolve import (
    parse_manual_full_export_file_to_json,
)

from . import CURRENT_DIR


def main() -> None:
    sample_file = CURRENT_DIR / "sample_output.txt"
    if not sample_file.exists():
        raise RuntimeError("请准备 HBase shell 输出文件 sample_output.txt")
    parse_manual_full_export_file_to_json(src_filepath=sample_file)


if __name__ == "__main__":
    main()
