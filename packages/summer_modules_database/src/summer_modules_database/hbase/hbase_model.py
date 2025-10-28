from typing import Optional, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime


class HBaseColumn(BaseModel):
    """HBase 单个列数据模型"""

    column_family: str = Field(..., description="列族名称, 如 `cf`")
    """列族名称,如 `cf`"""
    column_qualifier: str = Field(..., description="列限定符, 如 `type`")
    """列限定符,如 `type`"""
    timestamp: int = Field(..., description="时间戳")
    """时间戳"""
    value: Union[str, dict[str, Any], list[Any], int, float, bool] = Field(
        ..., description="列值，支持多种数据类型"
    )
    """列值，支持多种数据类型"""


# 单行记录模型
class HBaseRow(BaseModel):
    """HBase行记录模型"""

    row_key: str = Field(..., description="行键")
    """行键"""
    columns: list[HBaseColumn] = Field(
        default_factory=list, description="该行的所有列数据"
    )
    """该行的所有列数据"""

    def get_column_value(
        self, column_family: str, column_qualifier: str
    ) -> Optional[Any]:
        """获取指定列的值

        Args:
            column_family (str): 列族名称(如 `cf`)
            column_qualifier (str): 列限定符(如 `type`)
        Returns:
            Optional[Any]: 列的值，如果不存在则返回 None
        """
        for col in self.columns:
            if (
                col.column_family == column_family
                and col.column_qualifier == column_qualifier
            ):
                return col.value
        return None

    def get_columns_by_family(self, column_family: str) -> list[HBaseColumn]:
        """获取指定列族的所有列

        Args:
            column_family (str): 列族名称(如 `cf`)
        Returns:
            list[HBaseColumn]: 指定列族的所有列
        """
        return [col for col in self.columns if col.column_family == column_family]


# 扫描结果模型
class HBaseScanResult(BaseModel):
    """HBase扫描结果模型"""

    success: bool = Field(default=True, description="扫描是否成功")
    """扫描是否成功"""
    error_message: Optional[str] = Field(default=None, description="错误信息")
    """错误信息"""
    table_name: str = Field(..., description="表名")
    """表名"""
    command: Optional[str | list] = Field(default=None, description="执行的 HBase 命令")
    """执行的 HBase 命令"""
    row_count: int = Field(default=0, description="返回的行数")
    """返回的行数"""
    execution_time: Optional[float] = Field(default=None, description="执行时间（秒）")
    """执行时间（秒）"""
    rows: list[HBaseRow] = Field(default_factory=list, description="扫描到的行记录列表")
    """扫描到的行记录列表"""
    last_row_key: Optional[str] = Field(
        default=None, description="最后一行的行键(当设置了最大行数限制且被触发时会返回此值)"
    )
    """最后一行的行键(当设置了最大行数限制且被触发时会返回此值)"""

    def __post_init__(self):
        """初始化后处理"""
        self.row_count = len(self.rows)

    def get_row_by_key(self, row_key: str) -> Optional[HBaseRow]:
        """根据行键获取行记录

        Args:
            row_key (str): 行键
        Returns:
            Optional[HBaseRow]: 对应的行记录，如果不存在则返回 None
        """
        for row in self.rows:
            if row.row_key == row_key:
                return row
        return None

    def get_all_column_families(self) -> list[str]:
        """获取所有列族名称"""
        families = set()
        for row in self.rows:
            for col in row.columns:
                families.add(col.column_family)
        return list(families)


class ReconstructTruncatedLinesResult(BaseModel):
    """重建被截断行的结果模型"""

    original: str = Field(..., description="原始行内容")
    """原始行内容"""
    reconstructed: str = Field(..., description="重建后的完整行内容")
    """重建后的完整行内容"""
    command_line: Optional[str] = Field(
        default=None, description="重建的命令行（如果适用）"
    )
    """重建的命令行（如果适用）"""
    row_title_line: Optional[str] = Field(
        default=None, description="行标题行（如果适用）"
    )
    """行标题行（如果适用）"""
    data_lines: Optional[list[str]] = Field(
        default=None, description="数据行列表（如果适用）"
    )
    """数据行列表（如果适用）"""
    row_count_line: Optional[str] = Field(
        default=None, description="行计数行（如果适用）"
    )
    """行计数行（如果适用）"""
    execution_time_line: Optional[str] = Field(
        default=None, description="执行时间行（如果适用）"
    )
    """执行时间行（如果适用）"""
    subsequent_command_line: Optional[str] = Field(
        default=None, description="后续命令行（如果适用）"
    )
    """后续命令行（如果适用）"""

    success: bool = Field(default=True, description="是否成功重建")
    """是否成功重建"""
    error_message: Optional[str] = Field(default=None, description="错误信息（如果有）")
    """错误信息（如果有）"""


################## ==== 备用模型,暂不使用 ==== ##################
# 针对WHOIS数据的特化模型（可选）
class WHOISData(BaseModel):
    """WHOIS数据模型"""

    audit: Optional[dict[str, Any]] = None
    administrative_contact: Optional[dict[str, Any]] = Field(
        None, alias="administrativeContact"
    )
    technical_contact: Optional[dict[str, Any]] = Field(None, alias="technicalContact")
    registrant: Optional[dict[str, Any]] = None
    name_servers: Optional[dict[str, Any]] = Field(None, alias="nameServers")
    created_date: Optional[str] = Field(None, alias="createdDate")
    updated_date: Optional[str] = Field(None, alias="updatedDate")
    expires_date: Optional[str] = Field(None, alias="expiresDate")
    status: Optional[str] = None
    domain_name: Optional[str] = Field(None, alias="domainName")
    registrar_name: Optional[str] = Field(None, alias="registrarName")
    registrar_iana_id: Optional[str] = Field(None, alias="registrarIANAID")
    contact_email: Optional[str] = Field(None, alias="contactEmail")
    registry_data: Optional[dict[str, Any]] = Field(None, alias="registryData")
    raw_text: Optional[str] = Field(None, alias="rawText")

    class Config:
        allow_population_by_field_name = True


class WHOISRecord(HBaseRow):
    """WHOIS记录特化模型"""

    @property
    def record_type(self) -> Optional[str]:
        """获取记录类型"""
        return self.get_column_value("cf", "type")

    @property
    def whois_data(self) -> Optional[WHOISData]:
        """获取WHOIS数据"""
        whois_value = self.get_column_value("cf", "whois")
        if whois_value and isinstance(whois_value, dict):
            return WHOISData(**whois_value)
        return None

    @property
    def domain_name(self) -> Optional[str]:
        """从行键中提取域名"""
        # 假设行键格式为: domain-timestamp
        parts = self.row_key.split("-")
        if len(parts) >= 2:
            return "-".join(parts[:-1])
        return self.row_key


class WHOISScanResult(HBaseScanResult):
    """WHOIS扫描结果特化模型"""

    rows: list[WHOISRecord] = Field(default_factory=list, description="WHOIS记录列表")  # type: ignore

    def get_domains(self) -> list[str]:
        """获取所有域名"""
        domains = []
        for row in self.rows:
            if hasattr(row, "domain_name") and row.domain_name:
                domains.append(row.domain_name)
        return domains

    def get_records_by_type(self, record_type: str) -> list[WHOISRecord]:
        """根据记录类型过滤记录"""
        return [row for row in self.rows if row.record_type == record_type]
