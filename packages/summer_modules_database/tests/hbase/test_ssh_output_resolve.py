import json

from summer_modules_database.hbase.ssh_output_resolve import (
    format_hbase_shell_json_output,
)


def test_format_hbase_shell_json_output_handles_hex_escape() -> None:
    raw = '{"value": "Hello \\x57orld"}'
    result = format_hbase_shell_json_output(raw)
    assert result == {"value": "Hello World"}


def test_format_hbase_shell_json_output_invalid_json() -> None:
    assert format_hbase_shell_json_output("not json") is None
