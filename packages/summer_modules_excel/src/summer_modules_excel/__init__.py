def get_column_index_by_name(worksheet, column_name, header_row=1):
    """获取指定列名对应的列索引"""
    for col in range(1, worksheet.max_column + 1):
        if worksheet.cell(row=header_row, column=col).value == column_name:
            return col
    return None

def get_cell_value(worksheet, row_index, column_name, header_row=1):
    """读取指定行和列名的单元格值"""
    col_idx = get_column_index_by_name(worksheet, column_name, header_row)
    if col_idx:
        return worksheet.cell(row=row_index, column=col_idx).value
    return None

def set_cell_value(worksheet, row_index, column_name, value, header_row=1):
    """设置指定行和列名的单元格值"""
    col_idx = get_column_index_by_name(worksheet, column_name, header_row)
    if col_idx:
        worksheet.cell(row=row_index, column=col_idx).value = value
        return True
    return False
