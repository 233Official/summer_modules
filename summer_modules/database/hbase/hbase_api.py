import os
import json
import zlib
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Optional, Union
import threading
from configparser import ConfigParser
import json
import re

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

from summer_modules.database.hbase.hbase_model import (
    HBaseColumn,
    HBaseRow,
    HBaseScanResult,
    ReconstructTruncatedLinesResult,
)
from summer_modules.database.hbase.ssh_output_resolve import (
    parse_hbase_shell_scan_cmd_output,
)

from summer_modules.ssh import SSHConnection


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

        # 初始化 SSHConnection
        self.ssh_connection = SSHConnection(
            hostname=host,
            username=username,
            password=password,
        )

        # 建立连接
        self._connect()
        self.ssh_connection.connect(
            enbale_hbase_shell=True,
            terminal_width=1024,  # 终端宽度
            # terminal_width=1024 * 1024,  # 终端宽度
            # terminal_height=1024 * 1024,  # 终端高度
            terminal_height=1024,  # 终端高度
        )

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
    def create_table(self, table_name: str, column_families: list[str]) -> bool:
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
    def get_data_with_timerange(
        self,
        table_name: str,
        start_timestamp: int,
        end_timestamp: int,
        include_timestamp: bool = True,
    ) -> list[dict[str, Any]]:
        """获取指定表在指定时间范围内的数据
        务必注意，这个方法会进行全表扫描，可能会消耗大量资源和时间。

        注意：由于 HBase Thrift 接口的限制，此方法使用客户端过滤来实现时间范围查询。
        这意味着所有数据都会被扫描，然后在客户端进行时间戳过滤，性能可能不如原生的 TIMERANGE 查询。

        Args:
            table_name: 表名
            start_timestamp: 起始时间戳（毫秒，包含）
            end_timestamp: 结束时间戳（毫秒，包含）
            include_timestamp: 是否在结果中包含时间戳信息，默认为 True

        Returns:
            list[dict[str, Any]]: 指定时间范围内的所有数据列表
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return []

            table_name_bytes = table_name.encode()

            # 创建扫描对象
            scan = TScan()

            # 注意：HBase Thrift 接口可能不支持直接的时间范围过滤
            # 我们需要依赖客户端过滤来实现时间范围查询
            # 这是一个性能权衡，因为需要扫描所有数据然后过滤

            # 可以尝试使用一个更宽泛的时间戳过滤器，但这通常不如真正的 TIMERANGE 有效
            # 为了更好的性能，建议在可能的情况下设计包含时间信息的行键

            HBASE_LOGGER.warning(
                f"正在使用客户端时间范围过滤 [{start_timestamp}, {end_timestamp}]。"
                f"这可能比原生 TIMERANGE 查询慢，建议考虑优化行键设计。"
            )

            scanner_id = self._client.scannerOpenWithScan(table_name_bytes, scan, None)

            result = []
            total_scanned = 0
            filtered_count = 0
            row_list = self._client.scannerGetList(scanner_id, 1000)

            while row_list:
                for row in row_list:
                    total_scanned += 1
                    row_data = {"row_key": row.row.decode()}
                    has_valid_data = False

                    for column, cell in row.columns.items():
                        # 客户端时间戳过滤 - 确保在指定范围内
                        if not (start_timestamp <= cell.timestamp <= end_timestamp):
                            continue

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

                        # 根据参数决定是否包含时间戳信息
                        if include_timestamp:
                            row_data[cf][qualifier] = {
                                "value": value,
                                "timestamp": cell.timestamp,
                            }
                        else:
                            row_data[cf][qualifier] = value

                        has_valid_data = True

                    # 只有当行数据包含有效列时才添加到结果中
                    if has_valid_data:
                        result.append(row_data)
                        filtered_count += 1

                row_list = self._client.scannerGetList(scanner_id, 1000)

            self._client.scannerClose(scanner_id)

            HBASE_LOGGER.info(
                f"从表 {table_name} 扫描了 {total_scanned} 行，"
                f"时间范围 [{start_timestamp}, {end_timestamp}] 内有效数据 {filtered_count} 条"
            )
            return result

        except Exception as e:
            HBASE_LOGGER.error(
                f"获取表 {table_name} 时间范围 [{start_timestamp}, {end_timestamp}] 数据时出错: {e}"
            )
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_all_data(
        self, table_name: str, include_timestamp: bool = False
    ) -> list[dict[str, Any]]:
        """获取指定表的所有数据
        务必注意，这个方法会进行全表扫描，可能会消耗大量资源和时间。

        Args:
            table_name: 表名
            include_timestamp: 是否包含时间戳

        Returns:
            list[dict[str, Any]]: 所有数据的列表
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
    ) -> Optional[dict[str, Any]]:
        """获取指定行的数据

        Args:
            table_name: 表名
            row_key: 行键
            include_timestamp: 是否包含时间戳

        Returns:
            Optional[dict[str, Any]]: 行数据，如果不存在则返回 None
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
    ) -> list[dict[str, Any]]:
        """获取指定行范围的数据

        Args:
            table_name: 表名
            start_row: 起始行键（包含）
            stop_row: 结束行键（不包含）
            include_timestamp: 是否包含时间戳

        Returns:
            list[dict[str, Any]]: 行数据列表
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
        self, table_name: str, columns: list[str], include_timestamp: bool = False
    ) -> list[dict[str, Any]]:
        """获取指定列的数据

        Args:
            table_name: 表名
            columns: 列名列表，格式为 ['family:qualifier', ...]
            include_timestamp: 是否包含时间戳

        Returns:
            list[dict[str, Any]]: 数据列表
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
        columns: list[str],
        include_timestamp: bool = False,
    ) -> Optional[dict[str, Any]]:
        """获取指定行和列的数据

        Args:
            table_name: 表名
            row_key: 行键
            columns: 列名列表，格式为 ['family:qualifier', ...]
            include_timestamp: 是否包含时间戳

        Returns:
            Optional[dict[str, Any]]: 行数据，如果不存在则返回 None
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
        columns: list[str],
        include_timestamp: bool = False,
    ) -> list[dict[str, Any]]:
        """获取指定行范围和列的数据

        Args:
            table_name: 表名
            start_row: 起始行键（包含）
            stop_row: 结束行键（不包含）
            columns: 列名列表，格式为 ['family:qualifier', ...]
            include_timestamp: 是否包含时间戳

        Returns:
            list[dict[str, Any]]: 行数据列表
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
        self, table_name: str, row_key: str, data: dict[str, dict[str, Any]]
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
        self, table_name: str, rows_data: list[tuple[str, dict[str, dict[str, Any]]]]
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

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_data_with_timerange_optimized(
        self,
        table_name: str,
        start_timestamp: int,
        end_timestamp: int,
        row_key_prefix: Optional[str] = None,
        columns: Optional[list[str]] = None,
        include_timestamp: bool = True,
    ) -> list[dict[str, Any]]:
        """优化的时间范围查询方法

        这个方法提供了多种优化策略来减少全表扫描的性能影响(但是仍然不建议使用, 建议走下面的 SSHConnection 的方案)
        1. 支持行键前缀过滤
        2. 支持列过滤
        3. 提供更详细的性能统计

        注意：由于 HBase Thrift 接口限制，真正的 TIMERANGE 过滤无法直接实现。
        建议在表设计时将时间信息编码到行键中以获得最佳性能。

        Args:
            table_name: 表名
            start_timestamp: 起始时间戳（毫秒，包含）
            end_timestamp: 结束时间戳（毫秒，包含）
            row_key_prefix: 行键前缀过滤，可以显著减少扫描范围
            columns: 指定列过滤，格式为 ['family:qualifier', ...]
            include_timestamp: 是否在结果中包含时间戳信息

        Returns:
            list[dict[str, Any]]: 过滤后的数据列表
        """
        try:
            if not self.table_exists(table_name):
                HBASE_LOGGER.error(f"表 {table_name} 不存在")
                return []

            table_name_bytes = table_name.encode()

            # 创建扫描对象
            scan = TScan()

            # 如果提供了行键前缀，使用前缀扫描来减少扫描范围
            if row_key_prefix:
                scan.startRow = row_key_prefix.encode()
                # 计算前缀的结束范围
                prefix_bytes = row_key_prefix.encode()
                # 创建一个稍大的前缀作为停止点
                stop_prefix = prefix_bytes + b"\xff"
                scan.stopRow = stop_prefix
                HBASE_LOGGER.info(f"使用行键前缀过滤: {row_key_prefix}")

            # 如果指定了列，使用列过滤
            columns_bytes = None
            if columns:
                columns_bytes = [col.encode() for col in columns]
                HBASE_LOGGER.info(f"使用列过滤: {columns}")

            HBASE_LOGGER.warning(
                f"执行时间范围查询 [{start_timestamp}, {end_timestamp}]。"
                f"使用客户端过滤，性能可能受限。"
            )

            scanner_id = self._client.scannerOpenWithScan(
                table_name_bytes, scan, columns_bytes
            )

            result = []
            total_scanned = 0
            filtered_count = 0
            time_filtered_count = 0

            row_list = self._client.scannerGetList(scanner_id, 1000)

            while row_list:
                for row in row_list:
                    total_scanned += 1
                    row_data = {"row_key": row.row.decode()}
                    has_valid_data = False

                    for column, cell in row.columns.items():
                        # 客户端时间戳过滤
                        if not (start_timestamp <= cell.timestamp <= end_timestamp):
                            time_filtered_count += 1
                            continue

                        cf, qualifier = column.decode().split(":", 1)

                        try:
                            # 数据解码逻辑与原方法相同
                            value_str = cell.value.decode()
                            try:
                                value = json.loads(value_str)
                            except json.JSONDecodeError:
                                value = value_str
                        except UnicodeDecodeError:
                            try:
                                decompressed = zlib.decompress(cell.value)
                                try:
                                    value = json.loads(decompressed.decode())
                                except json.JSONDecodeError:
                                    value = decompressed.decode()
                            except Exception:
                                value = cell.value

                        if cf not in row_data:
                            row_data[cf] = {}

                        if include_timestamp:
                            row_data[cf][qualifier] = {
                                "value": value,
                                "timestamp": cell.timestamp,
                            }
                        else:
                            row_data[cf][qualifier] = value

                        has_valid_data = True

                    if has_valid_data:
                        result.append(row_data)
                        filtered_count += 1

                row_list = self._client.scannerGetList(scanner_id, 1000)

            self._client.scannerClose(scanner_id)

            HBASE_LOGGER.info(
                f"优化查询完成 - 表: {table_name}, "
                f"扫描行数: {total_scanned}, "
                f"时间过滤掉: {time_filtered_count}, "
                f"最终结果: {filtered_count} 条"
            )

            return result

        except Exception as e:
            HBASE_LOGGER.error(f"优化时间范围查询失败 - 表: {table_name}, 错误: {e}")
            self._reconnect()
            raise

    def calculate_reverse_timestamp(self, timestamp_ms: int) -> int:
        """计算反向时间戳

        Args:
            timestamp_ms: 正向时间戳（毫秒）

        Returns:
            int: 反向时间戳
        """
        JAVA_LONG_MAX = 9223372036854775807
        return JAVA_LONG_MAX - timestamp_ms

    def reverse_timestamp_to_normal(self, reverse_timestamp: int) -> int:
        """将反向时间戳转换为正常时间戳

        Args:
            reverse_timestamp: 反向时间戳

        Returns:
            int: 正常时间戳（毫秒）
        """
        JAVA_LONG_MAX = 9223372036854775807
        return JAVA_LONG_MAX - reverse_timestamp

    def count_rows_with_timerage_via_ssh(
        self,
        table_name: str,
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> Optional[int]:
        """计算指定表时间范围内的数据行数

        Args:
            table_name: 表名
            start_datetime: 起始日期时间
            end_datetime: 结束日期时间

        Returns:
            int: 符合条件的行数
        """
        if not self.table_exists(table_name):
            HBASE_LOGGER.error(f"表 {table_name} 不存在")
            return None
        HBASE_LOGGER.debug(
            f"start_datetime: {start_datetime}, end_datetime: {end_datetime}"
        )
        start_datetime_UTC = start_datetime.astimezone(ZoneInfo("UTC"))
        end_datetime_UTC = end_datetime.astimezone(ZoneInfo("UTC"))
        HBASE_LOGGER.debug(
            f"start_datetime_UTC: {start_datetime_UTC}, end_datetime_UTC: {end_datetime_UTC}"
        )

        start_timestamp = int(start_datetime_UTC.timestamp() * 1000)  # 转换为毫秒时间戳
        end_timestamp = int(end_datetime_UTC.timestamp() * 1000)  # 转换为毫秒时间戳
        HBASE_LOGGER.debug(
            f"start_timestamp: {start_timestamp}, end_timestamp: {end_timestamp}"
        )
        command = f"count '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}]}}"

        ssh_result = self.ssh_connection.execute_hbase_command(command)
        if not ssh_result.success:
            HBASE_LOGGER.error(
                f"通过 SSH 执行 HBase 命令失败: {ssh_result.error_message}"
            )
            return None

        output = ssh_result.output
        if not output:
            error_message = "SSH 执行 Hbase Shell 命令没有返回任何信息, 这并非正常情况, 请手动调试检查"
            HBASE_LOGGER.error(error_message)
            return None

        # 直接匹配 => 数字 的形式即为行数
        match = re.search(r"=> (\d+)", output)
        if not match:
            error_message = "无法从 HBase Shell 输出中解析行数"
            HBASE_LOGGER.error(error_message)
            return None

        row_count = int(match.group(1))
        HBASE_LOGGER.info(
            f"表 {table_name} 在时间范围 [{start_datetime_UTC}, {end_datetime_UTC}] 内的行数: {row_count}"
        )
        return row_count

    def get_data_with_timerage_via_ssh(
        self,
        table_name: str,
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> HBaseScanResult:
        """通过 SSH 连接获取指定时间范围内的数据

        Args:
            table_name: 表名
            start_date: 起始日期
            end_date: 结束日期

        Returns:
            HBaseScanResult: 包含扫描结果的对象
        """
        if not self.table_exists(table_name):
            HBASE_LOGGER.error(f"表 {table_name} 不存在")
            return HBaseScanResult(
                success=False,
                error_message=f"表 {table_name} 不存在",
                table_name=table_name,
            )

        HBASE_LOGGER.debug(
            f"start_datetime: {start_datetime}, end_datetime: {end_datetime}"
        )
        start_datetime_UTC = start_datetime.astimezone(ZoneInfo("UTC"))
        end_datetime_UTC = end_datetime.astimezone(ZoneInfo("UTC"))
        HBASE_LOGGER.debug(
            f"start_datetime_UTC: {start_datetime_UTC}, end_datetime_UTC: {end_datetime_UTC}"
        )

        start_timestamp = int(start_datetime_UTC.timestamp() * 1000)  # 转换为毫秒时间戳
        end_timestamp = int(end_datetime_UTC.timestamp() * 1000)  # 转换为毫秒时间戳
        HBASE_LOGGER.debug(
            f"start_timestamp: {start_timestamp}, end_timestamp: {end_timestamp}"
        )
        command = f"scan '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}]}}"
        # 先限制 2 条数据测试下基本功能
        # command = f"scan '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}],LIMIT => 2}}"
        # 限制 200 条数据测试下基本功能
        # command = f"scan '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}],LIMIT => 200}}"
        # 限制 1000,500 条数据测试下基本功能
        command = f"scan '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}],LIMIT => 1000}}"
        # 限制 100 条数据测试下基本功能
        # command = f"scan '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}],LIMIT => 100}}"

        ssh_result = self.ssh_connection.execute_hbase_command(
            command=command,
            # buffer_size= 1024, # 设置缓冲区大小为 1KB
            # buffer_size=102400 # 设置缓冲区大小为 100KB
            buffer_size=1024 * 1024,  # 设置缓冲区大小为 1MB
            # buffer_size= 1024 * 1024 * 1024,  # 设置缓冲区大小为 1GB
        )
        if not ssh_result.success:
            HBASE_LOGGER.error(
                f"通过 SSH 执行 HBase 命令失败: {ssh_result.error_message}"
            )
            return HBaseScanResult(
                success=False,
                error_message=ssh_result.error_message,
                table_name=table_name,
                command=command,
            )

        output = ssh_result.output
        if not output:
            error_message = "SSH 执行 Hbase Shell 命令没有返回任何信息, 这并非正常情况, 请手动调试检查"
            HBASE_LOGGER.error(error_message)
            return HBaseScanResult(
                success=False,
                table_name=table_name,
                command=command,
                error_message=error_message,
            )

        # 解析 HBase Shell 扫描命令的输出
        scan_result = parse_hbase_shell_scan_cmd_output(output)
        if not scan_result.success:
            HBASE_LOGGER.error(
                f"解析 HBase Shell 扫描命令输出失败: {scan_result.error_message}"
            )
            return scan_result

        # 对比 scan_result 中的 table_name 和 command 解析的是否与当前一致
        if scan_result.table_name != table_name or scan_result.command != command:
            error_message = (
                f"解析的表名或命令与预期不符: 解析结果表名 {scan_result.table_name}, "
                f"预期表名 {table_name}, 解析结果命令 {scan_result.command}, "
                f"预期命令 {command}"
            )
            HBASE_LOGGER.error(error_message)
            return HBaseScanResult(
                success=False,
                table_name=table_name,
                command=command,
                error_message=error_message,
            )

        # 返回解析后的结果
        return scan_result

    def close(self):
        """关闭 HBase 连接"""
        self._close()
