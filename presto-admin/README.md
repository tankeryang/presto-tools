# presto-admin

此脚本为 presto 的配置管理工具，目前仅实现 __catalog__ 的 __备份__ 和 __重载__，使用方法参考 __help & usage__

## help & usage

- first of all

```shell
# cd to presto-tools directory
> $ source venv/bin/activate
(venv) > $ cd presto-admin
```

进行 catalog 的管理之前，先做好当前 catalog 的备份工作，__将 presto 的`catalog`文件夹拷贝到`presto-admin`目录下__

接下来进行配置工作，在`presto-admin`下新建`config.ini`文件，配置`COORDINATOR`和`WORKER`的相关属性，目前可供配置如下

```ini
[COORDINATOR]
host1 = emr-header-1
user = # set your user name
password = # set your passwoed
catalog_path = /etc/ecm/presto-conf/catalog

[WORKER]
host1 = emr-worker-1
host2 = emr-worker-2
host3 = emr-worker-3
host4 = emr-worker-4
host5 = emr-worker-5
user = # set your user name
password = # set your passwoed
catalog_path = /etc/ecm/presto-conf/catalog
```

__注意：`COORDINATOR`和`WORKER`配置必须要有`user`，`password`，`catalog_path`参数__

配置完后，先进行 catalog 的备份

```shell
(venv) > $ python3 presto-admin.py -bc
```

此时会多出`catalog.bak`文件夹，为`catalog`的备份

- help:

```shell
(venv) > $ python3 presto-admin.py -h

usage: python3 presto-admin.py [-h] [--usage] [--backup-catalog]
                               [--reload-catalog] [--list-catalog]

This is a python manage script

optional arguments:
  -h, --help            show this help message and exit
  --usage, -u           show usage
  --backup-catalog, -bc
                        backup catalog
  --reload-catalog, -rc
                        reload catalog
  --list-catalog, -lc   list catalog file
```

- usage:

```shell
(venv) > $ python3 presto-admin.py -u

python prestoetl.py <option> [arguments]

for help
--------
python presto-admin.py -h

example
-------
python presto-admin.py --usage: show usage
```