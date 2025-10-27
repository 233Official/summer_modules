# summer_modules

233的Python工具箱 | Python Utility Collection by 233

---

## 简介

`summer_modules` 是一个面向日常开发、数据分析与安全自动化的 Python 工具箱，涵盖常用的通用工具、日志、Excel、Markdown、图表、网络安全、威胁情报、Web请求等模块。适合个人开发、自动化脚本、数据处理等多场景。

---

## 安装与依赖管理

本项目当前使用 [Poetry](https://python-poetry.org/) 进行依赖管理与打包。

```bash
# 推荐使用官方脚本安装 poetry
curl -sSL https://install.python-poetry.org | python3 -

# 安装依赖
poetry install

# 运行测试
poetry run pytest
```

> 计划切换到 [uv](https://github.com/astral-sh/uv) 进行依赖管理，并拆分为多个子模块，敬请关注后续更新。

---

## 项目结构与模块功能

项目结构简明如下：

```bash
├── config.toml            # 配置文件
├── pyproject.toml         # poetry项目配置文件
├── summer_modules         # 模块主目录
│   ├── ai                 # AI相关（如英译中）
│   ├── charts             # 图表绘制与可视化
│   ├── excel              # Excel 文件操作
│   ├── logger             # 彩色日志、Prefect日志
│   ├── markdown           # Markdown 编辑、图片托管
│   ├── security           # 漏洞信息聚合、威胁情报
│   ├── web_request_utils  # 随机 User-Agent、Web请求辅助
│   └── utils.py           # 通用工具函数
├── tests                  # 单元测试与用例
└── ...
```

主要模块功能：

- AI相关：如 `deepseek.py` 提供英译中等 AI 工具
- charts：数据可视化与图表绘制
- excel：Excel 文件读写、单元格操作、列名索引
- logger：自定义彩色日志、Prefect日志
- markdown：Markdown 文档处理、图片托管、OSS 支持
- security：漏洞信息聚合（ATT&CK、CNNVD、CVE、NVD、GitHub Nuclei等）、威胁情报（如 OTX）
- web_request_utils：随机 User-Agent 生成、Web请求辅助
- utils：常用工具函数（JSON、TXT 文件读写、目录遍历等）

---

## 快速上手

建议直接参考 `tests/` 目录下的测试用例，里面包含了各模块的典型用法示例。

---

## 贡献指南

1. Fork 本仓库并新建分支
2. 提交代码前请确保通过所有单元测试
3. 提交 PR 时请详细描述变更内容
4. 欢迎 issue 反馈与建议

---

## 版本管理与分支策略

- 遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/) 规范维护 CHANGELOG
- 重要版本均会打 tag 归档，如 `v1.0.0` 为大模块归档版本，后续将进行破坏式更新与模块拆分
- 主分支 `main` 保持稳定，开发分支建议以 `feature/`、`fix/` 前缀命名

---

## 联系方式 & 致谢

- 作者：233Official
- 邮箱：<ayusummer233@outlook.com>
- 欢迎关注、Star、交流！

---

## Changelog

详细变更请查阅 [CHANGELOG.md](CHANGELOG.md)。

---
