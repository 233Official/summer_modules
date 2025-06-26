import pytest
import json
import zlib
from unittest.mock import patch, MagicMock, call
from pathlib import Path

from summer_modules.database.hbase.hbase_api import HBaseAPI
from summer_modules.database.hbase.hbase.ttypes import (
    ColumnDescriptor,
    Mutation,
    TScan,
    TColumn,
    TRowResult,
    TCell,
)
from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG

# 测试数据
TEST_HOST = "localhost"
TEST_PORT = 9090
TEST_TABLE_NAME = "test_table"
TEST_ROW_KEY = "row1"
TEST_COLUMN_FAMILIES = ["cf1", "cf2"]
TEST_DATA = {
    "cf1": {"q1": "value1", "q2": {"nested": "value"}},
    "cf2": {"q1": "value3", "q2": [1, 2, 3]},
}


@pytest.fixture
def mock_hbase_client():
    """模拟 HBase 客户端"""
    with patch("summer_modules.database.hbase.hbase_api.Hbase.Client") as mock_client:
        with patch("summer_modules.database.hbase.hbase_api.THttpClient.THttpClient"):
            with patch(
                "summer_modules.database.hbase.hbase_api.TTransport.TBufferedTransport"
            ):
                with patch(
                    "summer_modules.database.hbase.hbase_api.TBinaryProtocol.TBinaryProtocol"
                ):
                    yield mock_client.return_value


@pytest.fixture
def hbase_api(mock_hbase_client):
    """初始化 HBaseAPI 实例"""
    api = HBaseAPI(TEST_HOST, TEST_PORT)
    return api


class TestHBaseAPI:
    """HBaseAPI 测试类"""

    def test_init_connection(self, hbase_api):
        """测试初始化连接"""
        assert hbase_api.host == TEST_HOST
        assert hbase_api.port == TEST_PORT
        assert hbase_api._client is not None

    def test_table_exists(self, hbase_api, mock_hbase_client):
        """测试表是否存在"""
        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]
        assert hbase_api.table_exists(TEST_TABLE_NAME) is True

        # 模拟表不存在
        mock_hbase_client.getTableNames.return_value = ["other_table".encode()]
        assert hbase_api.table_exists(TEST_TABLE_NAME) is False

    def test_create_table(self, hbase_api, mock_hbase_client):
        """测试创建表"""
        # 模拟表不存在
        mock_hbase_client.getTableNames.return_value = []

        # 调用创建表方法
        result = hbase_api.create_table(TEST_TABLE_NAME, TEST_COLUMN_FAMILIES)

        # 验证创建表调用
        mock_hbase_client.createTable.assert_called_once()
        assert result is True

    def test_delete_table(self, hbase_api, mock_hbase_client):
        """测试删除表"""
        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]

        # 调用删除表方法
        result = hbase_api.delete_table(TEST_TABLE_NAME)

        # 验证删除表调用
        mock_hbase_client.disableTable.assert_called_once_with(TEST_TABLE_NAME.encode())
        mock_hbase_client.deleteTable.assert_called_once_with(TEST_TABLE_NAME.encode())
        assert result is True

    def test_get_row(self, hbase_api, mock_hbase_client):
        """测试获取单行数据"""
        # 创建模拟响应
        mock_cells = {
            b"cf1:q1": TCell(value=b"value1", timestamp=123456),
            b"cf1:q2": TCell(
                value=json.dumps({"nested": "value"}).encode(), timestamp=123456
            ),
            b"cf2:q1": TCell(value=b"value3", timestamp=123456),
            b"cf2:q2": TCell(value=json.dumps([1, 2, 3]).encode(), timestamp=123456),
        }
        mock_row = TRowResult(row=TEST_ROW_KEY.encode(), columns=mock_cells)
        mock_hbase_client.getRow.return_value = [mock_row]

        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]

        # 调用获取行方法
        result = hbase_api.get_row(TEST_TABLE_NAME, TEST_ROW_KEY)

        # 验证结果
        assert result["row_key"] == TEST_ROW_KEY
        assert result["cf1"]["q1"] == "value1"
        assert result["cf1"]["q2"] == {"nested": "value"}
        assert result["cf2"]["q1"] == "value3"
        assert result["cf2"]["q2"] == [1, 2, 3]

    def test_put_row(self, hbase_api, mock_hbase_client):
        """测试写入一行数据"""
        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]

        # 调用写入行方法
        result = hbase_api.put_row(TEST_TABLE_NAME, TEST_ROW_KEY, TEST_DATA)

        # 验证写入调用
        mock_hbase_client.mutateRow.assert_called_once()
        assert result is True

    def test_get_rows(self, hbase_api, mock_hbase_client):
        """测试获取多行数据"""
        # 创建模拟响应
        mock_cells1 = {
            b"cf1:q1": TCell(value=b"value1", timestamp=123456),
            b"cf1:q2": TCell(
                value=json.dumps({"nested": "value"}).encode(), timestamp=123456
            ),
        }
        mock_cells2 = {
            b"cf1:q1": TCell(value=b"value2", timestamp=123456),
            b"cf2:q1": TCell(value=b"value3", timestamp=123456),
        }
        mock_row1 = TRowResult(row=b"row1", columns=mock_cells1)
        mock_row2 = TRowResult(row=b"row2", columns=mock_cells2)

        # 模拟Scanner返回结果
        mock_hbase_client.scannerOpenWithScan.return_value = 123  # scanner ID
        mock_hbase_client.scannerGetList.side_effect = [[mock_row1, mock_row2], []]

        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]

        # 调用获取行范围方法
        result = hbase_api.get_rows(TEST_TABLE_NAME, "row1", "row3")

        # 验证结果
        assert len(result) == 2
        assert result[0]["row_key"] == "row1"
        assert result[1]["row_key"] == "row2"
        assert result[0]["cf1"]["q1"] == "value1"
        assert result[1]["cf1"]["q1"] == "value2"

        # 验证Scanner关闭
        mock_hbase_client.scannerClose.assert_called_once_with(123)

    def test_put_rows(self, hbase_api, mock_hbase_client):
        """测试批量写入多行数据"""
        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]

        # 准备多行数据
        rows_data = [("row1", TEST_DATA), ("row2", TEST_DATA)]

        # 调用批量写入方法
        result = hbase_api.put_rows(TEST_TABLE_NAME, rows_data)

        # 验证写入调用（两次，每行一次）
        assert mock_hbase_client.mutateRow.call_count == 2
        assert result is True

    def test_reconnect_on_error(self, hbase_api, mock_hbase_client):
        """测试错误时重连"""
        # 模拟第一次调用失败，第二次成功
        mock_hbase_client.getTableNames.side_effect = [
            Exception("Connection error"),
            [TEST_TABLE_NAME.encode()],
        ]

        # 调用方法，应该触发重连并最终成功
        result = hbase_api.table_exists(TEST_TABLE_NAME)

        # 验证结果和重连
        assert result is True

    def test_get_row_with_columns(self, hbase_api, mock_hbase_client):
        """测试获取指定行和列的数据"""
        # 创建模拟响应
        mock_cells = {
            b"cf1:q1": TCell(value=b"value1", timestamp=123456),
            b"cf2:q1": TCell(value=b"value3", timestamp=123456),
        }
        mock_row = TRowResult(row=TEST_ROW_KEY.encode(), columns=mock_cells)
        mock_hbase_client.getRowWithColumns.return_value = [mock_row]

        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]

        # 调用获取行和列方法
        columns = ["cf1:q1", "cf2:q1"]
        result = hbase_api.get_row_with_columns(TEST_TABLE_NAME, TEST_ROW_KEY, columns)

        # 验证结果
        assert result["row_key"] == TEST_ROW_KEY
        assert result["cf1"]["q1"] == "value1"
        assert result["cf2"]["q1"] == "value3"

    def test_compressed_data(self, hbase_api, mock_hbase_client):
        """测试处理压缩数据"""
        # 创建压缩数据
        data = {"compressed": "value"}
        compressed_data = zlib.compress(json.dumps(data).encode())

        # 创建模拟响应
        mock_cells = {b"cf1:q1": TCell(value=compressed_data, timestamp=123456)}
        mock_row = TRowResult(row=TEST_ROW_KEY.encode(), columns=mock_cells)
        mock_hbase_client.getRow.return_value = [mock_row]

        # 模拟表存在
        mock_hbase_client.getTableNames.return_value = [TEST_TABLE_NAME.encode()]

        # 不再模拟 json.loads，让原始代码处理解压和解析
        result = hbase_api.get_row(TEST_TABLE_NAME, TEST_ROW_KEY)

        # 验证结果
        assert result["cf1"]["q1"] == data


if __name__ == "__main__":
    pytest.main(["-v", "test_hbase_api.py"])
