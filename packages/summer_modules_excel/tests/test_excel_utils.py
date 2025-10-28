from __future__ import annotations

import pytest
from openpyxl import Workbook

from summer_modules_excel import (
    get_cell_value,
    get_column_index_by_name,
    set_cell_value,
)


@pytest.fixture()
def sample_sheet():
    wb = Workbook()
    ws = wb.active
    ws.append(["Name", "Age", "City"])
    ws.append(["Alice", 30, "Beijing"])
    ws.append(["Bob", 25, "Shanghai"])
    return ws


def test_get_column_index_by_name(sample_sheet):
    assert get_column_index_by_name(sample_sheet, "Name") == 1
    assert get_column_index_by_name(sample_sheet, "age", case_sensitive=False) == 2
    assert get_column_index_by_name(sample_sheet, "NotExists") is None


def test_get_cell_value(sample_sheet):
    assert get_cell_value(sample_sheet, 2, "Age") == 30
    assert get_cell_value(sample_sheet, 3, "city", case_sensitive=False) == "Shanghai"
    assert get_cell_value(sample_sheet, 1, "Unknown") is None


def test_set_cell_value(sample_sheet):
    assert set_cell_value(sample_sheet, 2, "Age", 31)
    assert sample_sheet.cell(row=2, column=2).value == 31
    assert not set_cell_value(sample_sheet, 2, "Unknown", "value")


def test_invalid_indices_raise(sample_sheet):
    with pytest.raises(ValueError):
        get_cell_value(sample_sheet, 0, "Age")

    with pytest.raises(ValueError):
        set_cell_value(sample_sheet, 0, "Age", 20)

    with pytest.raises(ValueError):
        get_column_index_by_name(sample_sheet, "Age", header_row=0)
