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
│   ├── charts             # 图表相关模块
│   ├── excel              # excel相关模块
│   │   ├── __init__.py
│   ├── logger             # 自定义彩色日志模块
│   ├── markdown           # markdown 编辑模块
│   ├── security           # 安全相关模块
│   │   ├── vulnerability      # 漏洞信息相关模块
│   │   │   ├── __init__.py
│   │   │   ├── attck          # attck官网漏洞信息
│   │   │   ├── cnnvd          # CNNVD官网漏洞信息
│   │   │   ├── cnvd    
│   │   │   ├── cve            # CVE官网漏洞信息查询
│   │   │   ├── github_repo    # nuclei仓库模板信息查询
│   │   │   ├── nvd
│   │   ├── threat_intelligence  # 威胁情报相关模块
│   │   │   ├── otx           # OTX相关模块
│   ├── web_request_utils  # 随机 UA 生成器
│   │   ├── __init__.py
│   │   └── browsers.json
│   └──  utils.py           # 通用工具模块
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

### [0.1.3] - 2025-06-10

- 新增功能
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

### [0.1.2] - 2025-05-13

删除 Python 版本上限 3.14 的限制，作为发布库限制版本上限不太合适，Python版本通常有着良好的向后兼容性。

poetry 创建项目时也一般是只限制下限，如果这里库版本限制了上限，那么所有调用库的项目都需要限制上限，就很不方便。

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
