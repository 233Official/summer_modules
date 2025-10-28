```bash
# 下载 HBase 的 Thrift 定义文件
## wget
wget https://raw.githubusercontent.com/apache/hbase/master/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
# curl
curl -o Hbase.thrift https://raw.githubusercontent.com/apache/hbase/master/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift

# 在 macOS 上安装 Thrift 编译器(这里是Thrift version 0.22.0)
brew install thrift

# 使用 Thrift 编译器生成 Python 客户端代码
thrift --gen py Hbase.thrift

# 将生成的 gen-py/hbase 目录复制到项目中
```