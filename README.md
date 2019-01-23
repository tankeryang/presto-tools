# basic-build

> 数据平台基础工具

目前使用的脚本工具有:

- presto-etl

- presto-admin

脚本工具统一放在 __gateway__ 上

## Installation

### git (on `http://gitlab.fp.bd14.com`)

```shell
> $ git clone http://gitlab.fp.bd14.com/bigdata/basic-build.git
> $ virtualenv -p <path-to-python3-interpreter> --no-site-packages venv
> $ pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir -r requirements.txt
```

### git (on `https://code.aliyun.com`)

```shell
> $ git clone https://code.aliyun.com/trendy-bigdata/presto-tools
> $ virtualenv -p <path-to-python3-interpreter> --no-site-packages venv
> $ pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir -r requirements.txt
```