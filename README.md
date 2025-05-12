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
├── summer_modules         # 模块㊗主目录
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
