# summer_modules

233的Python工具箱

---

## 项目结构

```bash
├── CHANGELOG.md           # 更新日志
├── README.md              # 项目说明    
├── config copy.toml       # 配置文件（示例）
├── config.toml            # 配置文件
├── poetry.lock            # poetry依赖锁定文件
├── pyproject.toml         # poetry项目配置文件
├── summer_modules         # 模块主目录
│   ├── __init__.py
│   ├── ai
│   │   ├── __init__.py    
│   │   ├── deepseek.py    # deeepseek英译中
│   ├── excel              # excel相关模块
│   │   ├── __init__.py
│   ├── logger.py          # 自定义彩色日志模块
│   ├── utils.py           # 通用工具模块
│   ├── vulnerability      # 漏洞信息相关模块
│   │   ├── __init__.py
│   │   ├── attck          # attck官网漏洞信息
│   │   ├── cnnvd          # CNNVD官网漏洞信息
│   │   ├── cnvd    
│   │   ├── cve            # CVE官网漏洞信息查询
│   │   ├── github_repo    # nuclei仓库模板信息查询
│   │   └── nvd
│   └── web_request_utils  # 随机 UA 生成器
│       ├── __init__.py
│       └── browsers.json
├── tests
│   ├── __init__.py
│   ├── test.json
│   ├── test_main.py
│   └── test_oneline.json
```


---

## Changelog

所有项目的显著变更都将记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/)。

---

### [0.1.1] - 2025-05-12

更新 CHANGELOG

---

### [0.1.0] - 2025-05-12

### 新增
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
