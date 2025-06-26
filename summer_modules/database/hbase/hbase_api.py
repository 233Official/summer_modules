import os
import json
import zlib
from typing import Dict, Any, List, Optional, Union, Tuple
import threading
from configparser import ConfigParser

from thrift.transport import THttpClient
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from summer_modules.utils import retry
from summer_modules.database.hbase import HBASE_LOGGER
from summer_modules.database.hbase.hbase import Hbase
from summer_modules.database.hbase.hbase.ttypes import (
    ColumnDescriptor,
    Mutation,
    TScan,
    TColumn,
)


class HBaseAPI:
    """HBase API 封装类

    提供对 HBase 数据库的连接和操作功能，包括表管理和数据查询等。
    """

    def __init__(self, host: str, port: int, username: str = "", password: str = ""):
        """初始化 HBase API 连接

        Args:
            host: HBase 服务器地址
            port: HBase 服务器端口
            username: 用户名（如果需要认证）
            password: 密码（如果需要认证）
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.write_lock = threading.Lock()

        # 初始化连接属性
        self._transport = None
        self._protocol = None
        self._client_instance = None

        # 建立连接
        self._connect()

        HBASE_LOGGER.info(f"HBase API 初始化完成，连接到 {host}:{port}")

    @property
    def _client(self) -> "Hbase.Client":
        """获取 HBase 客户端，确保非空"""
        if self._client_instance is None:
            raise ValueError("HBase 客户端未初始化")
        return self._client_instance

    def _connect(self):
        """建立到 HBase 服务器的连接"""
        url = f"http://{self.host}:{self.port}"
        try:
            self._transport = THttpClient.THttpClient(url)
            if self.username and self.password:
                self._transport.setCustomHeaders(
                    {"Authorization": f"Basic {self.username}:{self.password}"}
                )
            self._transport = TTransport.TBufferedTransport(self._transport)
            self._protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
            self._client_instance = Hbase.Client(self._protocol)
            self._transport.open()
            HBASE_LOGGER.info("HBase 连接建立成功")
        except Exception as e:
            HBASE_LOGGER.error(f"HBase 连接失败: {e}")
            raise

    def _reconnect(self):
        """重新连接 HBase 服务器"""
        self._close()
        self._connect()
        HBASE_LOGGER.info("HBase 连接已重新建立")

    def _close(self):
        """关闭到 HBase 服务器的连接"""
        if self._transport and self._transport.isOpen():
            self._transport.close()
            HBASE_LOGGER.info("HBase 连接已关闭")

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在

        Args:
            table_name: 表名

        Returns:
            bool: 表是否存在
        """
        try:
            table_name_bytes = table_name.encode()
            if not self._client:
                HBASE_LOGGER.error("HBase 客户端未初始化，无法检查表是否存在")
                return False
            tables = self._client.getTableNames()
            return table_name_bytes in tables
        except Exception as e:
            HBASE_LOGGER.error(f"检查表 {table_name} 是否存在时出错: {e}")
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def create_table(self, table_name: str, column_families: List[str]) -> bool:
        """创建 HBase 表

        Args:
            table_name: 表名
            column_families: 列族列表

        Returns:
            bool: 表是否成功创建
        """
        try:
            if self.table_exists(table_name):
                HBASE_LOGGER.info(f"表 {table_name} 已存在")
                return True

            table_name_bytes = table_name.encode()
            column_families_bytes = [cf.encode() for cf in column_families]

            columns = [
                ColumnDescriptor(name=cf, maxVersions=1) for cf in column_families_bytes
            ]

            self._client.createTable(table_name_bytes, columns)
            HBASE_LOGGER.info(f"表 {table_name} 创建成功")
            return True
        except Exception as e:
            HBASE_LOGGER.error(f"创建表 {table_name} 失败: {e}")
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def delete_table(self, table_name: str) -> bool:
        """删除 HBase 表

        Args:
            table_name: 表名

        Returns:
            bool: 表是否成功删除
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.info(f"表 {table_name} 不存在，无需删除")
                return True

            table_name_bytes = table_name.encode()
            self._client.disableTable(table_name_bytes)
            self._client.deleteTable(table_name_bytes)
            HBASE_LOGGER.info(f"表 {table_name} 删除成功")
            return True
        except Exception as e:
            HBASE_LOGGER.error(f"删除表 {table_name} 失败: {e}")
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_all_data(
        self, table_name: str, include_timestamp: bool = False
    ) -> List[Dict[str, Any]]:
        """获取指定表的所有数据

        Args:
            table_name: 表名
            include_timestamp: 是否包含时间戳

        Returns:
            List[Dict[str, Any]]: 所有数据的列表
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return []

            table_name_bytes = table_name.encode()
            scan = TScan()
            scanner_id = self._client.scannerOpenWithScan(table_name_bytes, scan, None)

            result = []
            row_list = self._client.scannerGetList(scanner_id, 1000)

            while row_list:
                for row in row_list:
                    row_data = {"row_key": row.row.decode()}

                    for column, cell in row.columns.items():
                        cf, qualifier = column.decode().split(":", 1)
                        value = cell.value.decode()

                        try:
                            # 尝试解析 JSON
                            value = json.loads(value)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            try:
                                # 尝试解压
                                decompressed = zlib.decompress(cell.value)
                                value = json.loads(decompressed.decode())
                            except:
                                # 保持原始值
                                pass

                        if cf not in row_data:
                            row_data[cf] = {}

                        if include_timestamp:
                            row_data[cf][qualifier] = {
                                "value": value,
                                "timestamp": cell.timestamp,
                            }
                        else:
                            row_data[cf][qualifier] = value

                    result.append(row_data)

                row_list = self._client.scannerGetList(scanner_id, 1000)

            self._client.scannerClose(scanner_id)
            return result
        except Exception as e:
            HBASE_LOGGER.error(f"获取表 {table_name} 的所有数据时出错: {e}")
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_row(
        self, table_name: str, row_key: str, include_timestamp: bool = False
    ) -> Optional[Dict[str, Any]]:
        """获取指定行的数据

        Args:
            table_name: 表名
            row_key: 行键
            include_timestamp: 是否包含时间戳

        Returns:
            Optional[Dict[str, Any]]: 行数据，如果不存在则返回 None
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return None

            table_name_bytes = table_name.encode()
            row_key_bytes = row_key.encode()

            rows = self._client.getRow(table_name_bytes, row_key_bytes, None)

            if not rows:
                return None

            row = rows[0]
            row_data = {"row_key": row.row.decode()}

            for column, cell in row.columns.items():
                cf, qualifier = column.decode().split(":", 1)

                try:
                    # 先尝试解码为 UTF-8
                    value_str = cell.value.decode()

                    # 再尝试解析 JSON
                    try:
                        value = json.loads(value_str)
                    except json.JSONDecodeError:
                        # 不是 JSON，保留为字符串
                        value = value_str

                except UnicodeDecodeError:
                    # 解码失败，可能是压缩数据
                    try:
                        # 尝试解压
                        decompressed = zlib.decompress(cell.value)
                        try:
                            value = json.loads(decompressed.decode())
                        except json.JSONDecodeError:
                            # 解压后不是 JSON
                            value = decompressed.decode()
                    except Exception:
                        # 既不是 UTF-8 也不是压缩数据
                        value = cell.value  # 保留为原始字节

                if cf not in row_data:
                    row_data[cf] = {}

                if include_timestamp:
                    row_data[cf][qualifier] = {
                        "value": value,
                        "timestamp": cell.timestamp,
                    }
                else:
                    row_data[cf][qualifier] = value

            return row_data
        except Exception as e:
            HBASE_LOGGER.error(f"获取表 {table_name} 行 {row_key} 的数据时出错: {e}")
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_rows(
        self,
        table_name: str,
        start_row: str,
        stop_row: str,
        include_timestamp: bool = False,
    ) -> List[Dict[str, Any]]:
        """获取指定行范围的数据

        Args:
            table_name: 表名
            start_row: 起始行键（包含）
            stop_row: 结束行键（不包含）
            include_timestamp: 是否包含时间戳

        Returns:
            List[Dict[str, Any]]: 行数据列表
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return []

            table_name_bytes = table_name.encode()
            start_row_bytes = start_row.encode()

            scan = TScan(startRow=start_row_bytes)
            if stop_row:
                scan.stopRow = stop_row.encode()

            scanner_id = self._client.scannerOpenWithScan(table_name_bytes, scan, None)

            result = []
            row_list = self._client.scannerGetList(scanner_id, 1000)

            while row_list:
                for row in row_list:
                    row_data = {"row_key": row.row.decode()}

                    for column, cell in row.columns.items():
                        cf, qualifier = column.decode().split(":", 1)
                        value = cell.value.decode()

                        try:
                            # 尝试解析 JSON
                            value = json.loads(value)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            try:
                                # 尝试解压
                                decompressed = zlib.decompress(cell.value)
                                value = json.loads(decompressed.decode())
                            except:
                                # 保持原始值
                                pass

                        if cf not in row_data:
                            row_data[cf] = {}

                        if include_timestamp:
                            row_data[cf][qualifier] = {
                                "value": value,
                                "timestamp": cell.timestamp,
                            }
                        else:
                            row_data[cf][qualifier] = value

                    result.append(row_data)

                row_list = self._client.scannerGetList(scanner_id, 1000)

            self._client.scannerClose(scanner_id)
            return result
        except Exception as e:
            HBASE_LOGGER.error(
                f"获取表 {table_name} 行范围 {start_row} 到 {stop_row} 的数据时出错: {e}"
            )
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_columns(
        self, table_name: str, columns: List[str], include_timestamp: bool = False
    ) -> List[Dict[str, Any]]:
        """获取指定列的数据

        Args:
            table_name: 表名
            columns: 列名列表，格式为 ['family:qualifier', ...]
            include_timestamp: 是否包含时间戳

        Returns:
            List[Dict[str, Any]]: 数据列表
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return []

            table_name_bytes = table_name.encode()
            columns_bytes = [col.encode() for col in columns]

            scan = TScan()
            scanner_id = self._client.scannerOpenWithScan(
                table_name_bytes, scan, columns_bytes
            )

            result = []
            row_list = self._client.scannerGetList(scanner_id, 1000)

            while row_list:
                for row in row_list:
                    row_data = {"row_key": row.row.decode()}

                    for column, cell in row.columns.items():
                        cf, qualifier = column.decode().split(":", 1)
                        value = cell.value.decode()

                        try:
                            # 尝试解析 JSON
                            value = json.loads(value)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            try:
                                # 尝试解压
                                decompressed = zlib.decompress(cell.value)
                                value = json.loads(decompressed.decode())
                            except:
                                # 保持原始值
                                pass

                        if cf not in row_data:
                            row_data[cf] = {}

                        if include_timestamp:
                            row_data[cf][qualifier] = {
                                "value": value,
                                "timestamp": cell.timestamp,
                            }
                        else:
                            row_data[cf][qualifier] = value

                    result.append(row_data)

                row_list = self._client.scannerGetList(scanner_id, 1000)

            self._client.scannerClose(scanner_id)
            return result
        except Exception as e:
            HBASE_LOGGER.error(
                f"获取表 {table_name} 的指定列 {columns} 数据时出错: {e}"
            )
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_row_with_columns(
        self,
        table_name: str,
        row_key: str,
        columns: List[str],
        include_timestamp: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """获取指定行和列的数据

        Args:
            table_name: 表名
            row_key: 行键
            columns: 列名列表，格式为 ['family:qualifier', ...]
            include_timestamp: 是否包含时间戳

        Returns:
            Optional[Dict[str, Any]]: 行数据，如果不存在则返回 None
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return None

            table_name_bytes = table_name.encode()
            row_key_bytes = row_key.encode()
            columns_bytes = [col.encode() for col in columns]

            rows = self._client.getRowWithColumns(
                table_name_bytes, row_key_bytes, columns_bytes, None
            )

            if not rows:
                return None

            row = rows[0]
            row_data = {"row_key": row.row.decode()}

            for column, cell in row.columns.items():
                cf, qualifier = column.decode().split(":", 1)
                value = cell.value.decode()

                try:
                    # 尝试解析 JSON
                    value = json.loads(value)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    try:
                        # 尝试解压
                        decompressed = zlib.decompress(cell.value)
                        value = json.loads(decompressed.decode())
                    except:
                        # 保持原始值
                        pass

                if cf not in row_data:
                    row_data[cf] = {}

                if include_timestamp:
                    row_data[cf][qualifier] = {
                        "value": value,
                        "timestamp": cell.timestamp,
                    }
                else:
                    row_data[cf][qualifier] = value

            return row_data
        except Exception as e:
            HBASE_LOGGER.error(
                f"获取表 {table_name} 行 {row_key} 的指定列 {columns} 数据时出错: {e}"
            )
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_rows_with_columns(
        self,
        table_name: str,
        start_row: str,
        stop_row: str,
        columns: List[str],
        include_timestamp: bool = False,
    ) -> List[Dict[str, Any]]:
        """获取指定行范围和列的数据

        Args:
            table_name: 表名
            start_row: 起始行键（包含）
            stop_row: 结束行键（不包含）
            columns: 列名列表，格式为 ['family:qualifier', ...]
            include_timestamp: 是否包含时间戳

        Returns:
            List[Dict[str, Any]]: 行数据列表
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return []

            table_name_bytes = table_name.encode()
            start_row_bytes = start_row.encode()
            columns_bytes = [col.encode() for col in columns]

            scan = TScan(startRow=start_row_bytes)
            if stop_row:
                scan.stopRow = stop_row.encode()

            scanner_id = self._client.scannerOpenWithScan(
                table_name_bytes, scan, columns_bytes
            )

            result = []
            row_list = self._client.scannerGetList(scanner_id, 1000)

            while row_list:
                for row in row_list:
                    row_data = {"row_key": row.row.decode()}

                    for column, cell in row.columns.items():
                        cf, qualifier = column.decode().split(":", 1)
                        value = cell.value.decode()

                        try:
                            # 尝试解析 JSON
                            value = json.loads(value)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            try:
                                # 尝试解压
                                decompressed = zlib.decompress(cell.value)
                                value = json.loads(decompressed.decode())
                            except:
                                # 保持原始值
                                pass

                        if cf not in row_data:
                            row_data[cf] = {}

                        if include_timestamp:
                            row_data[cf][qualifier] = {
                                "value": value,
                                "timestamp": cell.timestamp,
                            }
                        else:
                            row_data[cf][qualifier] = value

                    result.append(row_data)

                row_list = self._client.scannerGetList(scanner_id, 1000)

            self._client.scannerClose(scanner_id)
            return result
        except Exception as e:
            HBASE_LOGGER.error(
                f"获取表 {table_name} 行范围 {start_row} 到 {stop_row} 的指定列 {columns} 数据时出错: {e}"
            )
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def put_row(
        self, table_name: str, row_key: str, data: Dict[str, Dict[str, Any]]
    ) -> bool:
        """写入一行数据

        Args:
            table_name: 表名
            row_key: 行键
            data: 数据，格式为 {'family': {'qualifier': value, ...}, ...}

        Returns:
            bool: 是否成功写入
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return False

            table_name_bytes = table_name.encode()
            row_key_bytes = row_key.encode()

            mutations = []

            for family, qualifiers in data.items():
                for qualifier, value in qualifiers.items():
                    # 如果值是字典或列表，转换为 JSON
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)

                    # 确保值是字符串类型
                    if not isinstance(value, str):
                        value = str(value)

                    column = f"{family}:{qualifier}".encode()
                    value_bytes = value.encode()

                    mutations.append(Mutation(column=column, value=value_bytes))

            self._client.mutateRow(table_name_bytes, row_key_bytes, mutations, None)
            return True
        except Exception as e:
            HBASE_LOGGER.error(f"写入表 {table_name} 行 {row_key} 的数据时出错: {e}")
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def put_rows(
        self, table_name: str, rows_data: List[Tuple[str, Dict[str, Dict[str, Any]]]]
    ) -> bool:
        """批量写入多行数据

        Args:
            table_name: 表名
            rows_data: 行数据列表，每个元素为 (row_key, data) 的元组
                       data 格式为 {'family': {'qualifier': value, ...}, ...}

        Returns:
            bool: 是否成功写入
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return False

            table_name_bytes = table_name.encode()

            batch_size = 100
            for i in range(0, len(rows_data), batch_size):
                batch = rows_data[i : i + batch_size]

                for row_key, data in batch:
                    row_key_bytes = row_key.encode()
                    mutations = []

                    for family, qualifiers in data.items():
                        for qualifier, value in qualifiers.items():
                            # 如果值是字典或列表，转换为 JSON
                            if isinstance(value, (dict, list)):
                                value = json.dumps(value)

                            # 确保值是字符串类型
                            if not isinstance(value, str):
                                value = str(value)

                            column = f"{family}:{qualifier}".encode()
                            value_bytes = value.encode()

                            mutations.append(Mutation(column=column, value=value_bytes))

                    self._client.mutateRow(
                        table_name_bytes, row_key_bytes, mutations, None
                    )

                HBASE_LOGGER.debug(f"成功写入 {len(batch)} 行数据到表 {table_name}")

            return True
        except Exception as e:
            HBASE_LOGGER.error(f"批量写入表 {table_name} 的数据时出错: {e}")
            self._reconnect()
            raise

    def close(self):
        """关闭 HBase 连接"""
        self._close()
