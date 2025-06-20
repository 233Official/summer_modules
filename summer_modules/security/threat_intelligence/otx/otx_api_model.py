from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime

from summer_modules.model import BaseResponseModel


class OTXIOCModel(BaseModel):
    """OTX IOC 模型"""

    access_groups: list[Any] = Field(
        default_factory=list, description="访问该IOC的组列表"
    )
    """访问该IOC的组列表"""
    access_reason: str = Field(default="", description="访问原因")
    """访问原因"""
    access_type: str = Field(default="public", description="访问类型, 如public")
    """访问类型, 如public"""
    content: str = Field(default="", description="IOC的内容")
    """IOC的内容"""
    created: datetime = Field(description="IOC创建时间")
    """IOC创建时间"""
    description: str = Field(default="", description="IOC描述")
    """IOC描述"""
    expiration: Optional[datetime] = Field(
        default=None, description="IOC过期时间, 如果不过期则为None"
    )
    """IOC过期时间, 如果不过期则为None"""
    indicator: str = Field(description="IOC的实际值, 如域名、IP、文件哈希等")
    """IOC的实际值, 如域名、IP、文件哈希等"""
    id: int = Field(description="IOC的唯一标识符")
    """IOC的唯一标识符"""
    is_active: int = Field(default=1, description="IOC是否处于活跃状态, 1表示活跃")
    """IOC是否处于活跃状态, 1表示活跃"""
    observations: int = Field(default=0, description="观察到该IOC的次数")
    """观察到该IOC的次数"""
    pulse_key: str = Field(description="关联的Pulse的唯一标识")
    """关联的Pulse的唯一标识"""
    role: Optional[str] = Field(
        default=None, description="IOC的角色, 如malware_hosting"
    )
    """IOC的角色, 如malware_hosting"""
    title: str = Field(default="", description="IOC标题")
    """IOC标题"""
    type: str = Field(description="IOC类型, 如domain、FileHash-MD5、URL等")
    """IOC类型, 如domain、FileHash-MD5、URL等"""
    false_positive: dict[str, Any] = Field(
        default_factory=dict, description="IOC的误报信息"
    )
    """误报信息"""
    metadata: dict[str, Any] = Field(default_factory=dict, description="IOC的元数据")
    """IOC的元数据"""
    slug: str = Field(description="IOC分类, 如file、domain、url等")
    """IOC分类, 如file、domain、url等"""

    def to_dict(self) -> dict[str, Any]:
        """将整个model转换为dict, 处理datetime对象"""
        model_dict = self.model_dump(by_alias=True, exclude_none=True)
        return self._process_datetime_in_dict(model_dict)

    def _process_datetime_in_dict(self, data: Any) -> Any:
        """递归处理字典中的datetime对象"""
        if isinstance(data, dict):
            return {
                key: self._process_datetime_in_dict(value)
                for key, value in data.items()
            }
        elif isinstance(data, list):
            return [self._process_datetime_in_dict(item) for item in data]
        elif isinstance(data, datetime):
            return data.isoformat()  # 将datetime转换为ISO格式字符串
        else:
            return data


class GetPulsesActiveIOCsResponseModel(BaseModel):
    """获取 Pulse 的活跃 IOC 响应模型"""

    count: int = Field(
        default=0,
        description="活跃 IOC 的数量",
    )
    """活跃 IOC 的数量"""

    active_iocs: list[OTXIOCModel] = Field(
        default_factory=list,
        description="活跃 IOC 的列表",
    )
    """活跃 IOC 的列表"""
