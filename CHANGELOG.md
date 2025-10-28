# Changelog

本文件记录了 summer_modules 项目的主要变更。

## [1.1.0] - 2025-10-28

> 本版本完成全面模块拆分，依赖管理切换为 `uv`，并引入自动化发布流程。

### Features

- 使用 `uv` 管理依赖与打包，提供统一脚本 `scripts/bump_dependencies.py`
- 拆分仓库为多个子包（core/ai/bot/charts/database/excel/markdown/prefect/security/ssh 等），主包 `summer-modules` 改为聚合依赖
- `summer-modules-security` 新增 `_storage` 抽象、OTX/CVE/CNNVD/Nuclei 等接口支持注入与离线缓存
- 新增覆盖各子包的示例与 pytest 测试，确保核心能力可单独回归

### Improvements

- README 重写：记录子包结构、uv 使用方式与发布流程
- CHANGELOG 按 Keep a Changelog 规范补充 v1.1.0 说明
- GitHub Actions：新增 tag 触发的自动发布工作流，串行发布所有子包与聚合包
- 清理旧示例缓存与 `__pycache__`，修复硬编码密钥/目录、日志初始化等问题

### Others

- 子包 `pyproject.toml` 对齐版本号并指向共享 `VERSION`
- `uv.lock`、根 `pyproject.toml` 纳入全部子包 workspace 配置

## [1.0.0] - 2025-10-27

> 本版本为归档版本，后续将进行破坏式更新和模块拆分。

### Features

- PostgreSQL 分区表管理与维护相关功能
- 企微 BOT 消息推送功能
- Gitlab OSS 支持与自定义文件路径
- Prefect work pool & deploy 检测
- Hbase 数据导出、时间范围查询、API 优化
- Markdown 编辑与图片托管相关功能增强
- 图表绘制（饼图、柱状图等）
- OTX API 及威胁情报相关功能
- 通用模块：TXT/JSON/JSONL 文件读写、时间戳与时区转换、文件名排序等

### Improvements

- 日志模块兼容性与类型提示优化
- Hbase、PostgreSQL、Markdown、OTX 等模块性能与结构优化
- Web请求与 User-Agent 相关工具增强
- 测试用例与代码结构整理

### Bug Fixes

- Logger、OTX、Hbase、SSH 等模块的若干 bug
- 依赖与类型注解问题

### Others

- 移除不合理的 info_color
- 清理冗余测试与配置文件
- 依赖管理与构建配置优化

---

## [0.1.3] - 2025-06-10

### Features (v0.1.3)

- 通用模块(utils.py)更新
  - `read_json_file_to_dict`: 新增从 JSON 文件读取字典的函数
  - `write_list_to_txt_file`: 新增将列表写入文本文件的函数
  - `read_txt_file_to_list`: 新增从文本文件读取列表的函数
  - `get_all_json_files`: 新增获取指定目录下所有 JSON 文件的函数
- 新增 OTX API(security/threat_intelligence/otx)
- 新增 markdown 操作模块(markdown)
- 新增图表模块(charts)
- Web 通用模块更新
  - `get_standard_domain_from_origin_domain`: 新增获取标准域名的函数

---

## [0.1.2] - 2025-05-13

### Bug Fixes (v0.1.2)

删除 Python 版本上限 3.14 的限制，作为发布库限制版本上限不太合适，Python版本通常有着良好的向后兼容性。

poetry 创建项目时也一般是只限制下限，如果这里库版本限制了上限，那么所有调用库的项目都需要限制上限，就很不方便。

---

## [0.1.1] - 2025-05-12

更新 CHANGELOG

---

## [0.1.0] - 2025-05-12

### Features (v0.1.0)

- 初始版本发布
- 包含如下模块
  - `ai.deepseek`: 英译中
  - `excel`: Excel 相关操作
    - `get_column_index_by_name`:获取指定列名对应的索引
    - `get_cell_value`: 获取指定行和列名的单元格值
    - `set_cell_value`: 设置指定行和列名的单元格值
  - `vulnerability`: 漏洞信息相关
    - `attck`：ATT&CK官网数据处理
    - `cnnvd`：CNNVD官网数据处理
    - `cve`：CVE官网数据处理以及指定编号CVE的POC/EXP查询
    - `github_repo.nuclei`: GitHub Nuclei 模板数据处理，以及查询指定CVE编号是否有对应的Nuclei模板
  - `web_request_utils.getUserAgent`: 获取随机的User-Agent
  - `logger`: 自定义颜色 logger
  - `utils`: 一些常用的工具函数
    - `write_dict_to_json_file`: 将字典写入 JSON 文件

---
