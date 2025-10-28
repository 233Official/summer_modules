from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class CommandResult(BaseModel):
    """单个命令的执行结果"""

    command: str = Field(..., description="执行的命令")
    output: str = Field(default="", description="命令的输出结果")
    index: int = Field(..., description="命令在序列中的索引（从0开始）")
    is_sensitive: bool = Field(default=False, description="是否为敏感命令（如密码输入）")


class InteractiveCommandResult(BaseModel):
    """交互式命令执行的完整结果"""

    success: bool = Field(..., description="命令序列是否成功执行")
    commands: List[str] = Field(default_factory=list, description="执行的命令列表")
    command_results: List[CommandResult] = Field(default_factory=list, description="每个命令的详细结果")
    command_outputs: Dict[str, str] = Field(default_factory=dict, description="命令到输出的映射字典")
    formatted_output: str = Field(default="", description="格式化的完整输出（用于控制台显示）")
    execution_time: float = Field(default=0.0, description="总执行时间（秒）")
    error_message: Optional[str] = Field(default=None, description="错误信息（如果有）")

    @property
    def command_count(self) -> int:
        """返回执行的命令数量"""
        return len(self.commands)

    def get_command_output(self, command: str) -> Optional[str]:
        """根据命令获取输出"""
        return self.command_outputs.get(command)

    def get_command_result_by_index(self, index: int) -> Optional[CommandResult]:
        """根据索引获取命令结果"""
        if 0 <= index < len(self.command_results):
            return self.command_results[index]
        return None

    def get_last_command_output(self) -> Optional[str]:
        """获取最后一个命令的输出"""
        if self.command_results:
            return self.command_results[-1].output
        return None

    def has_errors(self) -> bool:
        """检查是否有错误"""
        return self.error_message is not None


class SingleCommandResult(BaseModel):
    """单个简单命令的执行结果"""

    success: bool = Field(..., description="命令是否成功执行")
    command: str = Field(..., description="执行的命令")
    output: str = Field(default="", description="命令输出")
    exit_code: Optional[int] = Field(default=None, description="命令退出码")
    execution_time: float = Field(default=0.0, description="执行时间（秒）")
    error_message: Optional[str] = Field(default=None, description="错误信息（如果有）")

    def has_errors(self) -> bool:
        """检查是否有错误"""
        return not self.success or self.error_message is not None
