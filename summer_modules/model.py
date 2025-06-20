from typing import Generic, TypeVar, Optional, Dict, Any, Type, ClassVar
from pydantic import BaseModel, Field, model_validator
from datetime import datetime
import uuid

T = TypeVar("T")  # 定义泛型类型变量


class BaseResponseModel(BaseModel, Generic[T]):
    success: bool
    data: Optional[T] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    operation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    @model_validator(mode="after")
    def validate_success_data_consistency(self) -> "BaseResponseModel[T]":
        """验证success状态与data的一致性"""
        # 如果是错误响应，data应为None
        if self.success is False and self.data is not None:
            raise ValueError("错误响应中不应包含数据")

        # 如果是错误响应，必须有错误消息
        if self.success is False and not self.error_message:
            raise ValueError("错误响应必须包含错误消息")

        # 如果是成功响应，不应有错误消息和错误代码
        if self.success is True and (self.error_message or self.error_code):
            raise ValueError("成功响应不应包含错误信息")

        return self

    @classmethod
    def create_success(
        cls: Type["BaseResponseModel[T]"], data: Optional[T] = None, **kwargs: Any
    ) -> "BaseResponseModel[T]":
        """
        创建成功响应对象的工厂方法

        Args:
            data: 成功响应包含的数据
            **kwargs: 其他可选字段

        Returns:
            配置好的响应对象
        """
        return cls(success=True, data=data, **kwargs)

    @classmethod
    def create_error(
        cls: Type["BaseResponseModel[T]"],
        error_message: str,
        details: Optional[Dict[str, Any]] = None,
        error_code: Optional[str] = None,
        **kwargs: Any
    ) -> "BaseResponseModel[T]":
        """
        创建错误响应对象的工厂方法

        Args:
            error_code: 错误代码
            error_message: 错误消息
            details: 详细错误信息
            **kwargs: 其他可选字段

        Returns:
            配置好的错误响应对象
        """
        return cls(
            success=False,
            error_code=error_code,
            error_message=error_message,
            error_details=details,
            **kwargs
        )


class ExtendedResponseModel(BaseResponseModel[T], Generic[T]):
    """
    扩展响应模型，包含额外的元数据、分页信息和警告消息

    适用于复杂操作、批量处理和需要额外上下文的API响应
    """

    # 执行元数据
    duration_ms: Optional[int] = None  # 执行耗时(毫秒)
    source: Optional[str] = None  # 数据来源

    # 分页信息
    total_count: Optional[int] = None  # 总记录数
    page: Optional[int] = None  # 当前页码
    page_size: Optional[int] = None  # 每页记录数
    has_more: Optional[bool] = None  # 是否有更多数据

    # 警告信息
    warnings: Optional[list[str]] = None  # 警告消息列表

    # 额外信息
    context: Optional[Dict[str, Any]] = None  # 上下文信息
    tags: Optional[list[str]] = None  # 标签

    @classmethod
    def create_success(
        cls: Type["ExtendedResponseModel[T]"],
        data: Optional[T] = None,
        duration_ms: Optional[int] = None,
        warnings: Optional[list[str]] = None,
        **kwargs: Any
    ) -> "ExtendedResponseModel[T]":
        """
        创建扩展成功响应对象

        Args:
            data: 成功响应包含的数据
            duration_ms: 操作执行时间(毫秒)
            warnings: 警告消息列表
            **kwargs: 其他可选字段

        Returns:
            配置好的扩展响应对象
        """
        return cls(
            success=True,
            data=data,
            duration_ms=duration_ms,
            warnings=warnings,
            **kwargs
        )

    @classmethod
    def create_paginated_success(
        cls: Type["ExtendedResponseModel[T]"],
        data: Optional[T] = None,
        total_count: int = 0,
        page: int = 1,
        page_size: int = 20,
        **kwargs: Any
    ) -> "ExtendedResponseModel[T]":
        """
        创建带分页信息的成功响应对象

        Args:
            data: 成功响应包含的数据
            total_count: 总记录数
            page: 当前页码
            page_size: 每页记录数
            **kwargs: 其他可选字段

        Returns:
            配置好的带分页信息的响应对象
        """
        has_more = (page * page_size) < total_count

        return cls(
            success=True,
            data=data,
            total_count=total_count,
            page=page,
            page_size=page_size,
            has_more=has_more,
            **kwargs
        )

    def add_warning(self, warning: str) -> None:
        """
        添加警告消息

        Args:
            warning: 警告消息
        """
        if self.warnings is None:
            self.warnings = []
        self.warnings.append(warning)

    def add_context(self, key: str, value: Any) -> None:
        """
        添加上下文信息

        Args:
            key: 上下文键
            value: 上下文值
        """
        if self.context is None:
            self.context = {}
        self.context[key] = value
