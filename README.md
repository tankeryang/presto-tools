# presto-tools

> presto command line tools collection

Following cli tools are avaliable to use:

- __presto-etl__: a python cli script to execute ETL SQL

- __presto-admin__: a python cli script to manage presto cluster

## Installation

### git clone

```shell
> $ git clone https://github.com/tankeryang/presto-tools.git
> $ cd presto-tools
# path-to-python3-interpreter is the path to python3 interpreter :) e.g. /usr/local/bin/python3.6
> $ virtualenv -p <path-to-python3-interpreter> --no-site-packages venv
> $ source venv/bin/activate
(venv) > $ pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir -r requirements.txt
```