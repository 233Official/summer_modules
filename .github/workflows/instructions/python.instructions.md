---
description: "python 程序文件编码规范"
applyTo: "**/*.py, src/**/*.py, tests/**/*.py"
---

## 项目结构

- 使用 src 布局，如 `src/your_package_name/`
- 将测试放在与 `src/` 平行的 `tests/` 目录中
- 将示例放在 `examples/` 目录中
- 将文档放在 `docs/` 目录中
- 将配置保存在 `config/` 目录, 如果只有少量配置可以放在 `config.toml` 中或作为环境变量
- 使用 uv 管理 Python 项目依赖, 在 `pyproject.toml` 中管理依赖需求
- 将静态文件放在 `static/` 目录中
- 使用 `templates/` 目录存放 Jinja2 模板

## 代码风格

- 遵循 Black 代码格式化
- 使用 isort 进行导入排序
- 遵循 PEP 8 命名约定：
  - 函数和变量使用 snake_case
  - 类使用 PascalCase
  - 常量使用 UPPER_CASE
- 最大行长度为 88 个字符（Black 默认）
- 使用绝对导入而非相对导入
- 始终优先考虑代码的可读性和清晰度。
- 为每个函数编写清晰简洁的注释。
- 确保函数具有描述性的名称并包含类型提示。
- 保持适当的缩进（每级缩进使用 4 个空格）。

## 类型提示

- 为所有函数参数和返回值使用类型提示
- 从 `typing` 模块导入类型
- 使用 `Type | None` 而不是 `Optional[Type]`
- 使用 `TypeVar` 实现泛型类型
- 在 `types.py` 中定义自定义类型
- 使用 `Protocol` 实现鸭子类型
