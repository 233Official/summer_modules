import os
import json
import zlib
import time
from datetime import datetime
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
    def get_data_with_timerange(
        self, table_name: str, start_timestamp: int, end_timestamp: int, include_timestamp: bool = True
    ) -> List[Dict[str, Any]]:
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
            List[Dict[str, Any]]: 指定时间范围内的所有数据列表
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
            HBASE_LOGGER.error(f"获取表 {table_name} 时间范围 [{start_timestamp}, {end_timestamp}] 数据时出错: {e}")
            self._reconnect()
            raise

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_all_data(
        self, table_name: str, include_timestamp: bool = False
    ) -> List[Dict[str, Any]]:
        """获取指定表的所有数据
        务必注意，这个方法会进行全表扫描，可能会消耗大量资源和时间。

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

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_data_with_timerange_optimized(
        self, 
        table_name: str, 
        start_timestamp: int, 
        end_timestamp: int, 
        row_key_prefix: Optional[str] = None,
        columns: Optional[List[str]] = None,
        include_timestamp: bool = True
    ) -> List[Dict[str, Any]]:
        """优化的时间范围查询方法
        
        这个方法提供了多种优化策略来减少全表扫描的性能影响：
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
            List[Dict[str, Any]]: 过滤后的数据列表
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
                stop_prefix = prefix_bytes + b'\xff'
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
            
            scanner_id = self._client.scannerOpenWithScan(table_name_bytes, scan, columns_bytes)

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

    @retry(max_retries=3, delay=5, exceptions=(Exception,))
    def get_data_with_timerange_via_shell(
        self, 
        table_name: str, 
        start_timestamp: int, 
        end_timestamp: int, 
        ssh_host: str,
        ssh_user: str,
        ssh_password: Optional[str] = None,
        ssh_port: int = 22,
        hbase_shell_path: str = "hbase",
        limit: Optional[int] = None,
        timeout: int = 300
    ) -> List[Dict[str, Any]]:
        """通过 SSH + HBase Shell 实现真正的 TIMERANGE 查询
        
        使用 paramiko SSH 连接执行 HBase Shell 命令，比 subprocess 方案更可靠。
        这是目前最高效的时间范围查询方案，使用原生的 HBase TIMERANGE 功能。
        
        Args:
            table_name: 表名
            start_timestamp: 起始时间戳（毫秒）
            end_timestamp: 结束时间戳（毫秒）
            ssh_host: SSH 主机地址
            ssh_user: SSH 用户名
            ssh_password: SSH 密码（如果不使用密钥认证）
            ssh_port: SSH 端口，默认 22
            hbase_shell_path: HBase Shell 命令路径
            limit: 限制返回的行数（可选）
            timeout: 命令执行超时时间（秒）
            
        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        ssh_connection = None
        
        try:
            # 验证必要参数
            if not ssh_password:
                raise ValueError("SSH 密码不能为空，当前实现需要密码认证")
            
            # 建立 SSH 连接
            ssh_connection = SSHConnection(
                hostname=ssh_host,
                username=ssh_user,
                password=ssh_password,
                port=ssh_port
            )
            ssh_connection.connect()
            
            HBASE_LOGGER.info(
                f"SSH 连接成功，准备执行 HBase Shell 时间范围查询: "
                f"[{start_timestamp}, {end_timestamp}]"
            )
            
            # 构建 HBase Shell 命令
            limit_clause = f", LIMIT => {limit}" if limit else ""
            
            # 使用单行命令避免复杂的交互式输入
            hbase_command = (
                f"echo \"scan '{table_name}', {{"
                f"TIMERANGE => [{start_timestamp}, {end_timestamp}]{limit_clause}"
                f"}}\" | {hbase_shell_path} shell"
            )
            
            HBASE_LOGGER.info(f"执行 HBase Shell 命令: {hbase_command}")
            
            # 执行命令
            output = ssh_connection.execute_command(
                command=hbase_command,
                timeout=timeout
            )
            
            if output is None:
                raise Exception("HBase Shell 命令执行失败，未返回输出")
            
            HBASE_LOGGER.info(f"HBase Shell 命令执行完成，输出长度: {len(output)} 字符")
            
            # 解析输出结果
            results = self._parse_hbase_shell_output(output)
            
            # 如果没有找到数据，提供诊断信息
            if not results:
                HBASE_LOGGER.warning("⚠️  查询未返回任何数据")
                
                # 检查输出中是否包含 "0 row(s)" 表示查询成功但无数据
                if "0 row(s)" in output:
                    HBASE_LOGGER.info("✅ 查询执行成功，但指定时间范围内无数据")
                    start_dt = datetime.fromtimestamp(start_timestamp / 1000)
                    end_dt = datetime.fromtimestamp(end_timestamp / 1000)
                    HBASE_LOGGER.info(f"🔍 建议尝试更大的时间范围: {start_dt} - {end_dt}")
                else:
                    HBASE_LOGGER.warning("❓ 查询可能执行失败，建议检查HBase Shell输出")
                    
                # 记录部分输出用于调试
                output_preview = output[:500] if len(output) > 500 else output
                HBASE_LOGGER.debug(f"HBase Shell 输出预览: {output_preview}")
            
            HBASE_LOGGER.info(f"SSH + HBase Shell 查询完成，返回 {len(results)} 条记录")
            return results
                
        except Exception as e:
            HBASE_LOGGER.error(f"SSH + HBase Shell 查询失败: {e}")
            raise
            
        finally:
            # 确保关闭 SSH 连接
            if ssh_connection:
                try:
                    ssh_connection.close()
                except Exception as e:
                    HBASE_LOGGER.warning(f"关闭 SSH 连接时出错: {e}")

    def get_data_with_timerange_via_shell_interactive(
        self, 
        table_name: str, 
        start_timestamp: int, 
        end_timestamp: int, 
        ssh_host: str,
        ssh_user: str,
        ssh_password: Optional[str] = None,
        ssh_port: int = 22,
        hbase_shell_path: str = "hbase",
        limit: Optional[int] = None,
        timeout: int = 300
    ) -> List[Dict[str, Any]]:
        """通过 SSH + HBase Shell 交互式执行 TIMERANGE 查询
        
        使用交互式 SSH 连接执行 HBase Shell，适用于需要更复杂交互的场景。
        
        Args:
            参数说明同 get_data_with_timerange_via_shell
            
        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        ssh_connection = None
        
        try:
            # 验证必要参数
            if not ssh_password:
                raise ValueError("SSH 密码不能为空，当前实现需要密码认证")
            
            # 建立 SSH 连接
            ssh_connection = SSHConnection(
                hostname=ssh_host,
                username=ssh_user,
                password=ssh_password,
                port=ssh_port
            )
            ssh_connection.connect()
            
            HBASE_LOGGER.info(
                f"SSH 交互式连接成功，准备执行 HBase Shell: "
                f"表 {table_name}, 时间范围 [{start_timestamp}, {end_timestamp}]"
            )
            
            # 构建 HBase Shell 扫描命令
            limit_clause = f", LIMIT => {limit}" if limit else ""
            scan_command = (
                f"scan '{table_name}', {{"
                f"TIMERANGE => [{start_timestamp}, {end_timestamp}]{limit_clause}"
                f"}}"
            )
            
            # 使用交互式执行 HBase Shell
            commands = [
                f"{hbase_shell_path} shell",  # 启动 HBase Shell
                scan_command,                  # 执行扫描命令
                "exit"                        # 退出 HBase Shell
            ]
            
            HBASE_LOGGER.info(f"执行交互式 HBase Shell 命令序列")
            
            output = ssh_connection.execute_interactive_commands(
                commands=commands,
                timeout=timeout,
                wait_between_commands=1.0  # HBase Shell 需要更多时间启动
            )
            
            if output is None:
                raise Exception("HBase Shell 交互式命令执行失败，未返回输出")
            
            HBASE_LOGGER.info(f"HBase Shell 交互式执行完成，输出长度: {len(output)} 字符")
            
            # 解析输出结果
            results = self._parse_hbase_shell_output(output)
            
            HBASE_LOGGER.info(f"SSH + HBase Shell 交互式查询完成，返回 {len(results)} 条记录")
            return results
                
        except Exception as e:
            HBASE_LOGGER.error(f"SSH + HBase Shell 交互式查询失败: {e}")
            raise
            
        finally:
            # 确保关闭 SSH 连接
            if ssh_connection:
                try:
                    ssh_connection.close()
                except Exception as e:
                    HBASE_LOGGER.warning(f"关闭 SSH 连接时出错: {e}")

    def _parse_hbase_shell_output(self, output: str) -> List[Dict[str, Any]]:
        """解析 HBase Shell 输出
        
        改进的解析方法，更好地处理各种输出格式。
        
        Args:
            output: HBase Shell 的原始输出
            
        Returns:
            List[Dict[str, Any]]: 解析后的数据
        """
        if not output or not output.strip():
            HBASE_LOGGER.warning("HBase Shell 输出为空")
            return []
        
        lines = output.split('\n')
        records = []
        
        HBASE_LOGGER.debug(f"开始解析输出，共 {len(lines)} 行")
        
        # 查找数据行
        in_data_section = False
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            
            # 跳过空行
            if not line:
                continue
            
            # 检测数据部分开始
            if "ROW" in line and "COLUMN+CELL" in line:
                in_data_section = True
                HBASE_LOGGER.debug(f"检测到数据部分开始，行 {line_num}: {line}")
                continue
            
            # 检测数据部分结束
            if in_data_section and ("row(s)" in line or "Took " in line):
                in_data_section = False
                HBASE_LOGGER.debug(f"检测到数据部分结束，行 {line_num}: {line}")
                continue
            
            # 解析数据行
            if in_data_section and line:
                if " column=" in line and " timestamp=" in line:
                    try:
                        record = self._parse_hbase_shell_data_line(line)
                        if record:
                            records.append(record)
                            HBASE_LOGGER.debug(f"解析数据行 {line_num}: {record['row_key']}")
                    except Exception as e:
                        HBASE_LOGGER.warning(f"解析数据行失败，行 {line_num}: {e}")
                        HBASE_LOGGER.debug(f"问题行内容: {line}")
        
        HBASE_LOGGER.info(f"解析完成，找到 {len(records)} 条数据记录")
        return records

    def _parse_hbase_shell_data_line(self, line: str) -> Optional[Dict[str, Any]]:
        """解析单个 HBase Shell 数据行
        
        Args:
            line: 数据行字符串
            
        Returns:
            Optional[Dict[str, Any]]: 解析后的数据记录，失败返回 None
        """
        try:
            # 提取行键（第一个空格之前）
            parts = line.split(' ', 1)
            if len(parts) < 2:
                return None
            
            row_key = parts[0]
            rest = parts[1]
            
            # 提取列信息
            column_match = None
            timestamp_match = None
            value_match = None
            
            # 简单的字符串解析
            if " column=" in rest:
                column_start = rest.find(" column=") + 8
                column_end = rest.find(",", column_start)
                if column_end == -1:
                    column_end = rest.find(" ", column_start)
                column_match = rest[column_start:column_end] if column_end != -1 else rest[column_start:]
            
            if " timestamp=" in rest:
                timestamp_start = rest.find(" timestamp=") + 11
                timestamp_end = rest.find(",", timestamp_start)
                if timestamp_end == -1:
                    timestamp_end = rest.find(" ", timestamp_start)
                timestamp_str = rest[timestamp_start:timestamp_end] if timestamp_end != -1 else rest[timestamp_start:]
                try:
                    timestamp_match = int(timestamp_str.strip())
                except ValueError:
                    timestamp_match = timestamp_str.strip()
            
            if " value=" in rest:
                value_start = rest.find(" value=") + 7
                value_match = rest[value_start:]
            
            return {
                'row_key': row_key,
                'column': column_match,
                'timestamp': timestamp_match,
                'value': value_match
            }
            
        except Exception as e:
            HBASE_LOGGER.warning(f"解析数据行时出错: {e}")
            return None

    def analyze_hbase_shell_output(self, output: str, test_name: str = "HBase查询") -> Dict[str, Any]:
        """分析 HBase Shell 输出统计信息
        
        Args:
            output: HBase Shell 的原始输出
            test_name: 测试名称，用于日志
            
        Returns:
            Dict[str, Any]: 分析结果字典
        """
        result = {
            "status": "success",
            "test_name": test_name,
            "output_lines": len(output.split('\n')),
            "output_size": len(output),
            "total_rows": 0,
            "data_rows": 0,
            "execution_info": "N/A",
            "execution_seconds": None,
            "rows_per_second": None
        }
        
        lines = output.split('\n')
        
        # 查找执行结果信息（末尾的统计行）
        for line in lines:
            line = line.strip()
            
            # 查找类似 "50345 row(s)" 的统计信息
            if "row(s)" in line and ("Took" in line or "in" in line):
                # 提取行数，如 "50345 row(s) in 52.24 seconds" 或 "50345 row(s)"
                import re
                match = re.search(r'(\d+)\s+row\(s\)', line)
                if match:
                    result["total_rows"] = int(match.group(1))
                    result["execution_info"] = line
                    
                    # 提取执行时间
                    time_match = re.search(r'([\d.]+)\s+seconds', line)
                    if time_match:
                        result["execution_seconds"] = float(time_match.group(1))
                        if result["total_rows"] > 0:
                            result["rows_per_second"] = f"{result['total_rows'] / float(time_match.group(1)):.1f}"
                break
        
        # 计算实际的数据行数（包含 column= 的行）
        data_line_count = 0
        for line in lines:
            if " column=" in line and " timestamp=" in line:
                data_line_count += 1
        
        result["data_rows"] = data_line_count
        
        HBASE_LOGGER.info(f"{test_name} 分析结果:")
        for key, value in result.items():
            HBASE_LOGGER.info(f"   {key}: {value}")
        
        return result

    def analyze_full_scan_output(self, output: str) -> Dict[str, Any]:
        """分析全量扫描输出，提取关键信息
        
        Args:
            output: HBase Shell 的原始输出
            
        Returns:
            Dict[str, Any]: 分析结果字典
        """
        result = {
            "status": "success",
            "output_lines": len(output.split('\n')),
            "output_size": len(output),
            "total_rows": 0,
            "data_rows": 0,
            "execution_info": "N/A"
        }
        
        lines = output.split('\n')
        
        # 查找执行结果信息（末尾的统计行）
        for line in lines:
            line = line.strip()
            
            # 查找类似 "50345 row(s)" 的统计信息
            if "row(s)" in line and ("Took" in line or "in" in line):
                import re
                match = re.search(r'(\d+)\s+row\(s\)', line)
                if match:
                    result["total_rows"] = int(match.group(1))
                    result["execution_info"] = line
                    
                    # 提取执行时间
                    time_match = re.search(r'([\d.]+)\s+seconds', line)
                    if time_match:
                        result["execution_seconds"] = float(time_match.group(1))
                        result["rows_per_second"] = f"{result['total_rows'] / float(time_match.group(1)):.1f}"
                break
        
        # 计算实际的数据行数（包含 column= 的行）
        data_line_count = 0
        for line in lines:
            if " column=" in line and " timestamp=" in line:
                data_line_count += 1
        
        result["data_rows"] = data_line_count
        
        # 输出关键行用于调试
        HBASE_LOGGER.info("全量扫描输出关键信息:")
        
        # 显示开头几行
        HBASE_LOGGER.info("开头5行:")
        for i, line in enumerate(lines[:5]):
            HBASE_LOGGER.info(f"  [{i:2d}] {line[:100]}{'...' if len(line) > 100 else ''}")
        
        # 显示末尾几行（通常包含统计信息）
        HBASE_LOGGER.info("末尾10行:")
        for i, line in enumerate(lines[-10:]):
            line_num = len(lines) - 10 + i
            HBASE_LOGGER.info(f"  [{line_num:2d}] {line[:100]}{'...' if len(line) > 100 else ''}")
        
        return result

    def test_connectivity_via_ssh(
        self, 
        ssh_host: str,
        ssh_user: str,
        ssh_password: str,
        ssh_port: int = 22
    ) -> bool:
        """通过 SSH 测试 HBase 连接性
        
        Args:
            ssh_host: SSH 主机地址
            ssh_user: SSH 用户名  
            ssh_password: SSH 密码
            ssh_port: SSH 端口
            
        Returns:
            bool: 连接性测试是否成功
        """
        ssh_connection = None
        
        try:
            HBASE_LOGGER.info("🚀 执行 HBase SSH 连接性测试...")
            
            ssh_connection = SSHConnection(
                hostname=ssh_host,
                username=ssh_user,
                password=ssh_password,
                port=ssh_port
            )
            ssh_connection.connect()
            
            # 测试基本命令
            result = ssh_connection.execute_command("echo 'SSH连接正常'")
            HBASE_LOGGER.info(f"✅ SSH 连接测试: {result}")
            
            # 测试 HBase 可用性
            result = ssh_connection.execute_command("hbase version | head -1", timeout=15)
            if result:
                HBASE_LOGGER.info(f"✅ HBase 可用: {result}")
            else:
                HBASE_LOGGER.warning("⚠️  HBase 版本检查失败")
            
            # 测试交互式 HBase Shell
            output = ssh_connection.execute_interactive_commands(
                commands=["hbase shell", "status", "exit"],
                timeout=60,
                wait_between_commands=2.0
            )
            
            if output and "status" in output:
                HBASE_LOGGER.info("✅ HBase Shell 交互式测试成功")
            else:
                HBASE_LOGGER.warning("⚠️  HBase Shell 交互式测试异常")
            
            return True
            
        except Exception as e:
            HBASE_LOGGER.error(f"❌ HBase SSH 连接性测试失败: {e}")
            return False
            
        finally:
            if ssh_connection:
                try:
                    ssh_connection.close()
                except Exception:
                    pass

    def close(self):
        """关闭 HBase 连接"""
        self._close()

    # =================== HBase Shell 集成方法 ===================
    
    def parse_hbase_shell_output_improved(self, output: str) -> List[Dict[str, Any]]:
        """改进的 HBase Shell 输出解析器
        
        Args:
            output: HBase Shell 的原始输出
            
        Returns:
            List[Dict[str, Any]]: 解析后的数据记录列表
        """
        if not output:
            return []
        
        lines = output.split('\n')
        records = []
        
        HBASE_LOGGER.debug(f"开始解析输出，共 {len(lines)} 行")
        
        # 查找数据行
        in_data_section = False
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            
            # 跳过空行
            if not line:
                continue
            
            # 检测数据部分开始
            if "ROW" in line and "COLUMN+CELL" in line:
                in_data_section = True
                HBASE_LOGGER.debug(f"检测到数据部分开始，行 {line_num}: {line}")
                continue
            
            # 检测数据部分结束
            if in_data_section and ("row(s)" in line or "Took " in line):
                in_data_section = False
                HBASE_LOGGER.debug(f"检测到数据部分结束，行 {line_num}: {line}")
                continue
            
            # 解析数据行
            if in_data_section and line:
                if " column=" in line and " timestamp=" in line:
                    try:
                        record = self._parse_hbase_shell_data_line(line)
                        if record:
                            records.append(record)
                            HBASE_LOGGER.debug(f"解析数据行 {line_num}: {record['row_key']}")
                    except Exception as e:
                        HBASE_LOGGER.warning(f"解析数据行失败，行 {line_num}: {e}")
                        HBASE_LOGGER.debug(f"问题行内容: {line}")
        
        HBASE_LOGGER.info(f"解析完成，找到 {len(records)} 条数据记录")
        return records

    def _parse_hbase_shell_data_line(self, line: str) -> Optional[Dict[str, Any]]:
        """解析单个 HBase Shell 数据行
        
        Args:
            line: 数据行字符串
            
        Returns:
            Optional[Dict[str, Any]]: 解析后的数据记录，失败返回 None
        """
        try:
            # 提取行键（第一个空格之前）
            parts = line.split(' ', 1)
            if len(parts) < 2:
                return None
            
            row_key = parts[0]
            rest = parts[1]
            
            # 提取列信息
            column_match = None
            timestamp_match = None
            value_match = None
            
            # 简单的字符串解析
            if " column=" in rest:
                column_start = rest.find(" column=") + 8
                column_end = rest.find(",", column_start)
                if column_end == -1:
                    column_end = rest.find(" ", column_start)
                column_match = rest[column_start:column_end] if column_end != -1 else rest[column_start:]
            
            if " timestamp=" in rest:
                timestamp_start = rest.find(" timestamp=") + 11
                timestamp_end = rest.find(",", timestamp_start)
                if timestamp_end == -1:
                    timestamp_end = rest.find(" ", timestamp_start)
                timestamp_str = rest[timestamp_start:timestamp_end] if timestamp_end != -1 else rest[timestamp_start:]
                try:
                    timestamp_match = int(timestamp_str.strip())
                except ValueError:
                    timestamp_match = timestamp_str.strip()
            
            if " value=" in rest:
                value_start = rest.find(" value=") + 7
                value_match = rest[value_start:]
            
            return {
                'row_key': row_key,
                'column': column_match,
                'timestamp': timestamp_match,
                'value': value_match
            }
            
        except Exception as e:
            HBASE_LOGGER.warning(f"解析数据行时出错: {e}")
            return None

    def analyze_hbase_shell_output(self, output: str, test_name: str = "HBase查询") -> Dict[str, Any]:
        """分析 HBase Shell 输出统计信息
        
        Args:
            output: HBase Shell 的原始输出
            test_name: 测试名称，用于日志
            
        Returns:
            Dict[str, Any]: 分析结果字典
        """
        result = {
            "status": "success",
            "test_name": test_name,
            "output_lines": len(output.split('\n')),
            "output_size": len(output),
            "total_rows": 0,
            "data_rows": 0,
            "execution_info": "N/A",
            "execution_seconds": None,
            "rows_per_second": None
        }
        
        lines = output.split('\n')
        
        # 查找执行结果信息（末尾的统计行）
        for line in lines:
            line = line.strip()
            
            # 查找类似 "50345 row(s)" 的统计信息
            if "row(s)" in line and ("Took" in line or "in" in line):
                # 提取行数，如 "50345 row(s) in 52.24 seconds" 或 "50345 row(s)"
                import re
                match = re.search(r'(\d+)\s+row\(s\)', line)
                if match:
                    result["total_rows"] = int(match.group(1))
                    result["execution_info"] = line
                    
                    # 提取执行时间
                    time_match = re.search(r'([\d.]+)\s+seconds', line)
                    if time_match:
                        result["execution_seconds"] = float(time_match.group(1))
                        if result["total_rows"] > 0:
                            result["rows_per_second"] = f"{result['total_rows'] / float(time_match.group(1)):.1f}"
                break
        
        # 计算实际的数据行数（包含 column= 的行）
        data_line_count = 0
        for line in lines:
            if " column=" in line and " timestamp=" in line:
                data_line_count += 1
        
        result["data_rows"] = data_line_count
        
        HBASE_LOGGER.info(f"{test_name} 分析结果:")
        for key, value in result.items():
            HBASE_LOGGER.info(f"   {key}: {value}")
        
        return result

    def analyze_full_scan_output(self, output: str) -> Dict[str, Any]:
        """分析全量扫描输出，提取关键信息
        
        Args:
            output: HBase Shell 的原始输出
            
        Returns:
            Dict[str, Any]: 分析结果字典
        """
        result = {
            "status": "success",
            "output_lines": len(output.split('\n')),
            "output_size": len(output),
            "total_rows": 0,
            "data_rows": 0,
            "execution_info": "N/A"
        }
        
        lines = output.split('\n')
        
        # 查找执行结果信息（末尾的统计行）
        for line in lines:
            line = line.strip()
            
            # 查找类似 "50345 row(s)" 的统计信息
            if "row(s)" in line and ("Took" in line or "in" in line):
                import re
                match = re.search(r'(\d+)\s+row\(s\)', line)
                if match:
                    result["total_rows"] = int(match.group(1))
                    result["execution_info"] = line
                    
                    # 提取执行时间
                    time_match = re.search(r'([\d.]+)\s+seconds', line)
                    if time_match:
                        result["execution_seconds"] = float(time_match.group(1))
                        result["rows_per_second"] = f"{result['total_rows'] / float(time_match.group(1)):.1f}"
                break
        
        # 计算实际的数据行数（包含 column= 的行）
        data_line_count = 0
        for line in lines:
            if " column=" in line and " timestamp=" in line:
                data_line_count += 1
        
        result["data_rows"] = data_line_count
        
        # 输出关键行用于调试
        HBASE_LOGGER.info("全量扫描输出关键信息:")
        
        # 显示开头几行
        HBASE_LOGGER.info("开头5行:")
        for i, line in enumerate(lines[:5]):
            HBASE_LOGGER.info(f"  [{i:2d}] {line[:100]}{'...' if len(line) > 100 else ''}")
        
        # 显示末尾几行（通常包含统计信息）
        HBASE_LOGGER.info("末尾10行:")
        for i, line in enumerate(lines[-10:]):
            line_num = len(lines) - 10 + i
            HBASE_LOGGER.info(f"  [{line_num:2d}] {line[:100]}{'...' if len(line) > 100 else ''}")
        
        return result
