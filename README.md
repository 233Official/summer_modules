# summer_modules

233的Python工具箱 | Python Utility Collection by 233

---

## 简介

`summer_modules` 是一个面向日常开发、数据分析与安全自动化的 Python 工具箱，自 `v1.1.0` 起全面拆分为多个按需安装的子包（如 `summer-modules-core`、`summer-modules-ai` 等），通过聚合包 `summer-modules` 统一依赖。适合个人开发、自动化脚本、数据处理与安全分析等多场景。

---

## 安装与依赖管理

本项目使用 [uv](https://github.com/astral-sh/uv) 进行依赖管理与打包，建议 Python 版本 ≥ 3.11。

```bash
# 安装 uv（官方推荐方式）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 同步依赖并创建虚拟环境
uv sync

# 运行测试
uv run pytest
```

> 若只想安装某个子包，可直接 `pip install summer-modules-core` 等；若安装聚合包，则 `pip install summer-modules`。

---

## 项目结构与模块功能

自 `v1.1.0` 起，核心源码全部位于 `packages/` 下的子包中：

```bash
├── packages/
│   ├── summer_modules_core/      # 通用核心工具、日志、配置加载等
│   ├── summer_modules_ai/        # AI/Deepseek 相关封装
│   ├── summer_modules_bot/       # 企业微信 Bot 等消息推送
│   ├── summer_modules_markdown/  # Markdown / 图床 / GitLab 图像托管
│   ├── summer_modules_charts/    # 图表绘制组件
│   ├── summer_modules_excel/     # Excel 读写与列名操作
│   ├── summer_modules_database/  # PostgreSQL、HBase 等数据库工具
│   ├── summer_modules_prefect/   # Prefect 工作池/部署工具
│   ├── summer_modules_security/  # 漏洞信息、威胁情报、ATT&CK 等
│   └── summer_modules_ssh/       # SSH 命令执行、HBase Shell 辅助
├── scripts/                      # 辅助脚本（如版本依赖同步）
├── .github/workflows/            # CI / 发布流程
├── pyproject.toml                # 聚合包配置（依赖所有子包）
└── README.md / CHANGELOG.md / config.toml
```

常用子包一览：

| 包名 | 说明 |
| --- | --- |
| `summer-modules-core` | 通用工具、日志、配置加载、Web 请求辅助等基础能力 |
| `summer-modules-ai` | Deepseek 英译中等 AI 工具封装 |
| `summer-modules-bot` | 企业微信机器人等消息推送能力 |
| `summer-modules-markdown` | Markdown 处理与图床、GitLab Image Host 支持 |
| `summer-modules-charts` | 可视化与图表绘制工具 |
| `summer-modules-excel` | Excel 读写、列名查找、单元格操作 |
| `summer-modules-database` | PostgreSQL 分区、HBase API、数据导出等 |
| `summer-modules-prefect` | Prefect 工作池巡检、部署检测、日志桥接 |
| `summer-modules-security` | CVE/CNNVD/攻击矩阵解析、OTX 威胁情报、Nuclei 缓存等 |
| `summer-modules-ssh` | SSH 命令执行、交互式命令、HBase Shell 支持 |

---

## 快速上手（开发者）

```bash
# 克隆仓库后安装依赖
uv sync

# 运行单元测试
uv run pytest

# 进入某个子包示例
uv run python -m packages.summer_modules_security.examples.threat_intelligence.otx.example_otx_api
```

建议直接参考 `packages/<module>/tests/` 与 `examples/`，涵盖各模块的典型用法。

---

## 贡献指南

1. Fork 本仓库并新建分支
2. 提交代码前请确保通过所有单元测试
3. 提交 PR 时请详细描述变更内容
4. 欢迎 issue 反馈与建议

---

## 版本管理与发布流程

- 遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/) 维护 `CHANGELOG.md`
- 从 `v1.1.0` 开始：更新根目录 `VERSION` → 执行 `scripts/bump_dependencies.py` → 推送 `v*` tag
- GitHub Actions 会在推送 tag 后自动执行测试并发布所有子包及聚合包到 PyPI（需配置 `PYPI_TOKEN`）
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
