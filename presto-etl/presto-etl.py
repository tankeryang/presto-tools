import os
import sys
import argparse
import logging
import prestodb
import requests
import pandas as pd
import itertools as it
from sqlalchemy import create_engine


class PrestoETL():
    """
    Presto ETL 工具类

    **Basic**

    这个类为 Presto ETL 的工具基类，可单独执行，一般以配合 azkaban
    进行 ETL 的调度，具体使用方法参考 **Usage**

    **Usage**

    - python script:
        先实例化 PrestoETL 类:
        ```
        if __name__ == '__main__':
            executor = PrestoETL()
            executor.execute()
        ```
    
    - Command:
        执行: python3 prestoetl.py -h 查看参数用法
        特性: 
            使用 argparse 进行参数解析
            选项前需加 '--': [ --presto.host ... ]
            --sql.name 可接受多个参数: --sql.name create_table fully ..
            --placeholder.config 可接受多个参数: --placeholder.config fully:fully-placeholder fully-fully-placeholder_2 ..

    .. version v2.0
    """

    # 执行脚本必须要带的参数
    NECESSARY_ARGS = {
        '--presto.host': 'presto_host',
        '--presto.port': 'presto_port',
        '--presto.user': 'presto_user',
        '--presto.catalog': 'presto_catalog',
        '--presto.schema': 'presto_schema',
        '--sql.url.prefix': 'sql_url_prefix',
        '--sql.dir': 'sql_dir',
        '--sql.names': 'sql_names',
    }

    # 可选参数
    # placeholder为中间变量，具体意义可参考方法 `get_placeholdes()` 的注释
    OPTIONAL_ARGS = {
        '--placeholder.config': 'placeholder_config',
    }

    USAGE = """
    python prestoetl.py <option> [arguments]

    for help
    --------
    python presto-etl.py -h

    example
    -------
    python prestoetl.py \\
        --presto.host 10.10.22.5 \\
        --presto.port 10300 \\
        --presto.user dev \\
        --presto.catalog dev_hive \\
        --presto.schema ods_test \\
        --sql.url.prefix http://gitlab.company.com/group/repo/raw/branch/sql/etl/dwh/ods/test \\
        --sql.dir table_name \\
        --sql.names create fully

    example for azkaban properties
    ------------------------------
    presto.host=10.10.22.5
    presto.port=10300
    presto.user=dev
    presto.catalog=dev_hive
    presto.schema=ods_test
    git.branch=dev
    sql.url.prefix=http://gitlab.company.com/group/repo/raw/${git.branch}/sql/etl/dwh/ods/test
    sql.dir=test
    sql.names=create fully

    python.etl.dir=/opt

    cmd=python3 ${python.etl.dir}/presto-etl.py \\
        --presto.host ${presto.host} \\
        --presto.port ${presto.port} \\
        --presto.user ${presto.user} \\
        --presto.catalog ${presto.catalog} \\
        --presto.schema ${presto.schema} \\
        --sql.url.prefix ${sql.url.prefix} \\
        --sql.dir ${sql.dir} \\
        --sql.names ${sql.names}
    """

    def __init__(self):
        """
        初始化时将参数通过 self.__set_args() 绑定到 self.__args 变量上
        同时初始化 session 以作为请求 url 的会话
        """
        self.__args = self.__set_args()
        self.__check_args()

        self.__session = self.__set_session()
        self.__sql_file = {}
        self.__placeholder_config = {}
        self.__placeholder_group = {}
 

    @property
    def args_dict(self):
        return self.__args_dict

    @property
    def sql_file(self):
        return self.__sql_file
        

    def __set_args(self):
        """
        设置参数选项
        """
        parser = argparse.ArgumentParser(prog="python3 prestoetl.py", description="This is a python etl script")

        # set usage
        parser.add_argument('--usage', action='store_true', dest='usage', default=False, help="show usage")

        # set necessary arguments
        parser.add_argument(
            '--presto.host', action='store', dest='presto_host',
            help="set presto host"
        )
        parser.add_argument('--presto.port', action='store', dest='presto_port',type=int,  help="set presto port")
        parser.add_argument('--presto.user', action='store', dest='presto_user', help="set presto user")
        parser.add_argument('--presto.catalog', action='store', dest='presto_catalog', help="set presto catalog")
        parser.add_argument('--presto.schema', action='store', dest='presto_schema', help="set presto schema")
        parser.add_argument(
            '--sql.url.prefix', action='store', dest='sql_url_prefix',
            help="set the gitlab url (route to the system name [e.g. crm, mms, fpos etc.]) for sql file"
        )
        parser.add_argument('--sql.dir', action='store', dest='sql_dir', help="set the parent diretory for sql file")
        parser.add_argument(
            '--sql.names', action='store', dest='sql_names', nargs='*',
            help="set the sql file name for sql file, avalible to recieve multiple argment"
        )

        # set optinal arguments
        parser.add_argument(
            '--placeholder.config', action='store', dest='placeholder_config', nargs='*',
            help="set the placeholder config"
        )

        args = parser.parse_args()

        # set args_dict
        args_key = list(map(lambda kv: kv[0], args._get_kwargs()))
        args_value = list(map(lambda kv: kv[1], args._get_kwargs()))

        self.__args_dict = dict(zip(args_key, args_value))

        return args


    def __check_args(self):
        """
        参数检查
        """
        # check usage
        if self.__args.usage is True:
            self.show_usage()
            sys.exit(0)

        # check necesary arguments
        for necessary_arg in PrestoETL.NECESSARY_ARGS.values():
            if self.__args_dict[necessary_arg] is None:
                logging.error(
                    "Please provide all necessary arguments: {}".format(str(list(PrestoETL.NECESSARY_ARGS.keys())))
                )
                sys.exit(1)


    def __set_session(self):
        """
        设置session
        """
        session = requests.session()
        request_retry = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount('https://', request_retry)
        session.mount('http://', request_retry)

        return session


    def __get_presto_connection(self):
        return prestodb.dbapi.connect(
            host=self.__args.presto_host,
            port=self.__args.presto_port,
            user=self.__args.presto_user,
            catalog=self.__args.presto_catalog,
            schema=self.__args.presto_schema
        )


    def __get_presto_engine(self):
        return create_engine('presto://{user}@{host}:{port}/{catalog}/{schema}'.format(
            user=self.__args.presto_user,
            host=self.__args.presto_host,
            port=self.__args.presto_port,
            catalog=self.__args.presto_catalog,
            schema=self.__args.presto_schema
        ))


    def get_sql(self, sql_name):
        """
        获取 sql_name 对应的 sql 脚本内容

        :params sql_name: sql 脚本名
        :return response.text: 请求 url 返回的 sql 文本
        """
        sql_url = "{}/{}/{}.sql".format(
            self.__args.sql_url_prefix,
            self.__args.sql_dir,
            sql_name
        )
        response = self.__session.get(sql_url)
        if response.status_code == 200:
            return response.text.strip('\n').strip()
        else:
            logging.error("{sql_url}: {status_code}, {reason}".format(
                    sql_url=sql_url, status_code=response.status_code, reason=response.reason
                ))
            sys.exit(1)


    def get_sql_file(self):
        """
        获取 self.__sql_file<dict>
        
        .. note:
            self.__sql_file 为 dict 类型，格式为 {sql_name: sql_text}
        """
        print(self.__args.sql_names)
        for sql_name in self.__args.sql_names:
            print(sql_name)
            self.__sql_file[sql_name] = self.get_sql(sql_name)


    def exec_sql(self, presto_cursor, sql):
        """
        执行 sql

        :params presto_cursor: prestodb.dbapi.connection.cursor
        :params sql: sql text
        """
        for sql in sql.split(';'):
            sql = sql.strip('\n').strip()
            if sql != '':
                print("Execute sql:")
                print(sql)
                presto_cursor.execute(sql)
                print("Results: " + str(presto_cursor.fetchall()))
            else:
                pass

    
    def get_placeholder_config(self, presto_engine):
        """
        获取 placeholder 填充配置
        在脚本选项中，--placeholder.config 的参数格式应为 ==> 被填充的sql名:获取填充内容的sql名

        .. 例如:

            --placeholder.config fully:fully-placeholders

        其中获取填充内容的sql必须为以下形式，column 为填充的 key，值必须为 array 类型:

        ``` fully-placeholders.sql
        select array[1, 2] AS times, array['tom'] AS names;
        ```
         times  | names
        --------+-------
         [1, 2] | [tom]


        生成的 placeholder_config 就是:

        placeholder_config = {
            'fully': {times': [1, 2], 'names': ['tom']}
        }

        形成的组合就是: ['1', 'tom'], ['2', 'tom']

        里面的值分别填充到 fully.sql 里的 {times}, {names}, 则 fully.sql 会被执行 2 次
        """

        if self.__args.placeholder_config is not None:

            for pc in self.__args.placeholder_config:
                pc_list = pc.split(':')

                # 判断参数是否正确
                if len(pc_list) < 2:
                    logging.error("--placeholder.config error. the args form must be <sql_name>:<placeholder_sql_name>")
                    sys.exit(0)
                
                sql_name = pc_list[0]
                placeholder_sql_name = pc_list[1]

                placeholder_key_values = pd.read_sql(
                    sql=self.get_sql(placeholder_sql_name).strip(';'),
                    con=presto_engine.connect()
                ).to_dict(orient='records')[0]

                if sql_name not in self.__placeholder_config.keys():
                    self.__placeholder_config[sql_name] = placeholder_key_values
                else:
                    self.__placeholder_config[sql_name].update(placeholder_key_values)

        else:
            pass
    

    def get_placeholder_group(self):
        """
        获取 placeholder 填充组合
        根据 placeholder_config 得出
        """
        for sql_name in self.__placeholder_config.keys():

            self.__placeholder_group[sql_name] = {}

            placeholder_keys_list = list(self.__placeholder_config[sql_name].keys())
            placeholder_values_group_list = [
                list(group) for group in it.product(*list(self.__placeholder_config[sql_name].values()))
            ]

            self.__placeholder_group[sql_name]['keys'] = placeholder_keys_list
            self.__placeholder_group[sql_name]['values'] = placeholder_values_group_list


    def exec_sql_with_placeholders(self, presto_cursor, sql_name):
        """
        将 placeholder 的值填充到 sql 字符串里，并执行

        :params presto_cursor: prestodb.dbapi.connect.cursor
        :params sql_name: sql 名
        """
        for values in self.__placeholder_group[sql_name]['values']:
            fill_dict = dict(zip(self.__placeholder_group[sql_name]['keys'], values))
            sql = self.__sql_file[sql_name]
            
            for key in fill_dict.keys():
                sql = sql.replace('{' + key + '}', str(fill_dict[key]))
            
            self.exec_sql(presto_cursor, sql)


    def execute(self):
        """
        执行
        """
        presto_cursor = self.__get_presto_connection().cursor()
        presto_engine = self.__get_presto_engine()
        
        self.get_sql_file()
        self.get_placeholder_config(presto_engine)
        print(self.__sql_file.keys())
        if len(self.__placeholder_config) != 0:
            self.get_placeholder_group()

        if len(self.__placeholder_group) != 0:
            for sql_name in self.__sql_file.keys():
                if sql_name in self.__placeholder_group.keys():
                    self.exec_sql_with_placeholders(presto_cursor, sql_name)
                else:
                    self.exec_sql(presto_cursor, self.__sql_file[sql_name])
        else:
            for sql_name in self.__sql_file.keys():
                self.exec_sql(presto_cursor, self.__sql_file[sql_name])

        print("============= Finish =============")

    
    def show_usage(self):
        if self.__args.usage is True:
            print(PrestoETL.USAGE)

        
    def test(self):
        print(self.args_dict)


if __name__ == '__main__':
    executor = PrestoETL()
    executor.execute()
    # executor.test()
