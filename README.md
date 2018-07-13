# presto-etl-tools

用 Python 实现一些基于 presto 的 ETL 工具脚本

## presqoop.py

准备实现一个基于 presto 的类 sqoop 的工具，支持从不同数据源无脑导入导出数据

## prestoetl.py

因为公司目前架构大概这样:

- OLAP 选用的 SQL 执行引擎为 [Presto](https://prestodb.io/)
- 调度工具为 [Azkaban](https://azkaban.github.io/)
- 所有 ETL 相关 SQL 单独维护在 GitLab 的一个 repo 上
- 数仓的结构做了分层: `ods`, `cdm`, `ads`，SQL 脚本文件的命名为 `fully*`, `increment*`，对应 __全量__，__增量__，这种结构同时体现在维护 SQL 脚本的 repo 的目录结构上

因此需要一个带参数的脚本型的工具来通过请求 GitLab 对应 SQL 的 url，然后通过 presto-python-client 发送到 presto 执行，azkaban 调度时只需配置好相关的参数即可。
因为公司一些项目的 ETL 需求比较刁钻，而 presto 只能执行 SQL，因此脚本的参数会有些繁杂，特别是 `--placeholder.*` 相关的参数，有觉得不解的查看代码的注释即可

### help & usage

- help:

```s
$ python3 prestoetl.py -h

usage: python3 sql-flow-excutor.py [-h] [--usage] [--presto.host PRESTO_HOST]
                                   [--presto.port PRESTO_PORT]
                                   [--presto.user PRESTO_USER]
                                   [--presto.catalog PRESTO_CATALOG]
                                   [--presto.schema PRESTO_SCHEMA]
                                   [--sql.url.prefix SQL_URL_PREFIX]
                                   [--sql.dir SQL_DIR]
                                   [--sql.names [SQL_NAMES [SQL_NAMES ...]]]
                                   [--placeholder.sql PLACEHOLDER_SQL]
                                   [--placeholder.loop [PLACEHOLDER_LOOP [PLACEHOLDER_LOOP ...]]]
                                   [--placeholder.save [PLACEHOLDER_SAVE [PLACEHOLDER_SAVE ...]]]
                                   [--placeholder.save.id PLACEHOLDER_SAVE_ID]
                                   [--placeholder.loop.value.separator PLACEHOLDER_LOOP_VALUE_SEPARATOR]

This is a python etl script

optional arguments:
  -h, --help            show this help message and exit
  --usage               show usage
  --presto.host PRESTO_HOST
                        set presto host
  --presto.port PRESTO_PORT
                        set presto port
  --presto.user PRESTO_USER
                        set presto user
  --presto.catalog PRESTO_CATALOG
                        set presto catalog
  --presto.schema PRESTO_SCHEMA
                        set presto schema
  --sql.url.prefix SQL_URL_PREFIX
                        set the gitlab url (route to the system name [e.g.
                        crm, mms, fpos etc.]) for sql file
  --sql.dir SQL_DIR     set the parent diretory for sql file
  --sql.names [SQL_NAMES [SQL_NAMES ...]]
                        set the sql file name for sql file, avalible to
                        recieve multiple argment
  --placeholder.sql PLACEHOLDER_SQL
                        set the sql file name which provide placeholder
  --placeholder.loop [PLACEHOLDER_LOOP [PLACEHOLDER_LOOP ...]]
                        set the placeholder and sql file which have to fill
                        into it by loopping
  --placeholder.save [PLACEHOLDER_SAVE [PLACEHOLDER_SAVE ...]]
                        save the placeholder to mysql
  --placeholder.save.id PLACEHOLDER_SAVE_ID
                        set the id to which placeholder you saved
  --placeholder.loop.value.separator PLACEHOLDER_LOOP_VALUE_SEPARATOR
                        set the placeholder loop value separator e.g.
                        --placeholder.loop.value.separator '~'

```

- usage:

```s
$ python3 prestoetl.py --usage

    python prestoetl.py <command> [arguments]

    for help
    --------
    python prestoetl.py -h

    example
    -------
    python prestoetl.py \
        --presto.host 10.10.22.5 \
        --presto.port 10300 \
        --presto.user dev \
        --presto.catalog dev_hive \
        --presto.schema ods_crm \
        --sql.url.prefix http://gitlab.project.company.com/big-data/file-repo/raw/master/sql/etl/data_warehouse/ods/crm \
        --sql.dir member_info \
        --sql.name increment

    example for azkaban properties
    ------------------------------
    presto.host=10.10.22.5
    presto.port=10300
    presto.user=dev
    presto.catalog=dev_hive
    presto.schema=ods_crm
    git.branch=dev
    sql.url.prefix=http://gitlab.project.company.com/big-data/file-repo/raw/${git.branch}/sql/etl/data_warehouse/ods/crm
    sql.dir=member_info
    sql.name=increment

    etl.script.dir=/opt

    cmd=python3 ${etl.script.dir}/prestoetl.py \
        --presto.host ${presto.host} \
        --presto.port ${presto.port} \
        --presto.user ${presto.user} \
        --presto.catalog ${presto.catalog} \
        --presto.schema ${presto.schema} \
        --sql.url.prefix ${sql.url.prefix} \
        --sql.dir ${sql.dir} \
        --sql.name ${sql.name}
```