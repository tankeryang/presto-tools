import os
import sys
import argparse
import logging
import prestodb
import pymysql
import requests
import json
import time


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
            --placeholder.loop 可接受多个参数: --placeholder.loop placeholder1:sql1 placeholder2:sql1 placeholder3:sql2 ..

    .. version v1.0
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
        '--placeholder.sql': 'placeholder_sql',
        '--placeholder.loop': 'placeholder_loop',
        '--placeholder.save': 'placeholder_save',
        '--placeholder.save_id': 'placeholder_save_id',
        '--placeholder.loop.value.separator': 'placeholder_loop_value_separator'
    }

    USAGE = """
    python prestoetl.py <option> [arguments]

    for help
    --------
    python prestoetl.py -h

    example
    -------
    python prestoetl.py \\
        --presto.host 10.10.22.5 \\
        --presto.port 10300 \\
        --presto.user dev \\
        --presto.catalog dev_hive \\
        --presto.schema ods_crm \\
        --sql.url.prefix http://gitlab.project.company.com/big-data/file-repo/raw/master/sql/etl/data_warehouse/ods/crm \\
        --sql.dir member_info \\
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

    cmd=python3 ${etl.script.dir}/prestoetl.py \\
        --presto.host ${presto.host} \\
        --presto.port ${presto.port} \\
        --presto.user ${presto.user} \\
        --presto.catalog ${presto.catalog} \\
        --presto.schema ${presto.schema} \\
        --sql.url.prefix ${sql.url.prefix} \\
        --sql.dir ${sql.dir} \\
        --sql.name ${sql.name}
    """

    def __init__(self):
        """
        初始化时将参数通过 self.__set_args() 绑定到 self.__args 变量上
        同时初始化 session 以作为请求 url 的会话
        """
        self.__args = self.__set_args()
        self.__check_args()

        self.__session = self.__set_session()
        # self.__logger = self.__set_logger()
        self.__sql_file = {}
        self.__placeholders = {}
        self.__placeholder_loop = {}
 

    @property
    def args_dict(self):
        return self.__args_dict

    @property
    def sql_file(self):
        return self.__sql_file

    @property
    def placeholders(self):
        return self.__placeholders

    @property
    def placeholder_loop(self):
        return placeholder_loop
        

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
            '--placeholder.sql', action='store', dest='placeholder_sql',
            help="set the sql file name which provide placeholder"
        )
        parser.add_argument(
            '--placeholder.loop', action='store', dest='placeholder_loop', nargs='*',
            help="set the placeholder and sql file which have to fill into it by loopping"
        )
        parser.add_argument(
            '--placeholder.save', action='store', dest='placeholder_save', nargs='*',
            help="save the placeholder to mysql"
        )
        parser.add_argument(
            '--placeholder.save.id', action='store', dest='placeholder_save_id',
            help="set the id to which placeholder you saved"
        )
        parser.add_argument(
            '--placeholder.loop.value.separator', action='store', dest='placeholder_loop_value_separator', type=str,
            default=',',
            help="set the placeholder loop value separator e.g. --placeholder.loop.value.separator '~'"
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
        for necessary_arg in prestoetl.NECESSARY_ARGS.values():
            if self.__args_dict[necessary_arg] is None:
                logging.error(
                    "Please provide all necessary arguments: {}".format(str(PrestoETL.NECESSARY_ARGS.keys()))
                )
                sys.exit(1)

        # check optional arguments
        if self.__args_dict['placeholder_save'] is not None and self.__args_dict['placeholder_save_id'] is None:
            logging.error("When --placeholder.save is provided, --placeholder.save.id must provided too.")
            sys.exit(1)


    def __set_session(self):
        """
        设置session
        """
        session = requests.session()
        request_retry = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount('https://',request_retry)
        session.mount('http://',request_retry)

        return session

    
    # def __set_logger(self):
    #     ## 定义Logger对象
    #     logger = logging.getLogger()
    #     logger.setLevel(level=logging.INFO)

    #     logfile = os.path.abspath(os.path.join(os.path.dirname(__file__), 'etc', 'log', 'etl.log'))

    #     if not os.path.exists(logfile):
    #         os.mknod(logfile)
    #         os.chmod(logfile, 777)

    #     formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    #     ## 获取文件logger句柄
    #     fh = logging.FileHandler(logfile, mode='a')
    #     fh.setLevel(level=logging.INFO)
    #     fh.setFormatter(formatter)  
    #     ## 获取终端logger句柄
    #     ch = logging.StreamHandler()  
    #     ch.setLevel(logging.INFO)
    #     ch.setFormatter(formatter)
    #     ## 添加至logger 
    #     logger.addHandler(fh)  
    #     logger.addHandler(ch)

    #     return logger


    def __get_presto_connection(self):
        return prestodb.dbapi.connect(
            host=self.__args.presto_host,
            port=self.__args.presto_port,
            user=self.__args.presto_user,
            catalog=self.__args.presto_catalog,
            schema=self.__args.presto_schema,
        )


    def get_sql(self, sql_url):
        """
        获取sql脚本内容

        :params sql_url:
            根据 --sql.url.prefix 和 --sql.name 的参数拼接得来

        :return response.text:
            请求 url 返回的sql文本
        """
        response = self.__session.get(sql_url)
        if response.status_code == 200:
            return response.text
        else:
            logging.error(
                "{sql_url}: {status_code}, {reason}".format(
                    sql_url=sql_url, status_code=response.status_code, reason=response.reason
                )
            )
            sys.exit(1)


    def get_sql_file(self):
        """
        获取 self.__sql_file<dict>
        
        .. note:
            self.__sql_file 为 dict 类型，格式为 {sql_name: sql_text}
        """
        sql_file = {}
        for sql_name in self.__args.sql_names:
            sql_url = "{}/{}/{}.sql".format(
                self.__args.sql_url_prefix,
                self.__args.sql_dir,
                sql_name
            )
            sql_file[sql_name] = self.get_sql(sql_url)
        self.__sql_file = sql_file


    def exec_sql(self, presto_cursor, sql):
        print("Execute sql: \n")
        print("{} \n\n".format(sql))
        presto_cursor.execute(sql)


    def get_placeholders(self, presto_cursor):
        """
        通过执行 --placeholder.sql 的 sql 脚本获取需要填充到 sql 里的中间变量 self.__placeholders<dict>

        :params presto_cursor:
            presto 连接器的游标，根据 self.__get_presto_connection().cursor() 而来，
            作为 self.exec_sql 的参数

        .. note:
            self.__placeholders 为 dict 类型，格式为 {placeholder_name: placeholder_value}
            e.g. {'max_id': '666666'}
        """
        placeholder = {}

        if self.__args.placeholder_sql is not None:
            sql_url = "{}/{}/{}.sql".format(
                self.__args.sql_url_prefix,
                self.__args.sql_dir,
                self.__args.placeholder_sql
            )
            sqls = self.get_sql(sql_url).split(';')

            logging.info("Get placeholder from {}".format(sql_url))

            if sqls:
                for sql in sqls:
                    sql = sql.strip()
                    if sql:
                        self.exec_sql(presto_cursor, sql)
                        time.sleep(1.5)
                        result = presto_cursor.fetchone()
                        print(result)
                        if result:
                            i = 0
                            while i < len(result):
                                placeholders[result[i]] = result[i + 1]
                                i += 2
            self.__placeholders = placeholders


    def fill_placeholders(self, sql_name):
        """
        将 self.__placeholders 对应的值填充到 sql 里

        :params sql_name:
            根据 --sql.name 参数而来，同时也是 self.__sql_file<dict> 的 key，
            通过它来获取到对应的 sql 脚本内容 (即 self.__sql_file<dict> 的 value)

        .. note:
            这个操作将更新 sql_name 对应的 sql 内容，即 self.__sql_file[sql_name]
        """
        for key in self.__placeholders:
            self.__sql_file[sql_name] = self.__sql_file[sql_name].replace(
                '{' + key + '}', str(self.__placeholders[key])
            )


    def exec_sql_ignore_result(self, presto_cursor, sql_name):
        """
        将 sql 语句发送到 presto 执行

        :params presto_cursor:
            presto 连接器的游标，根据 self.__get_presto_connection().cursor() 而来，
            作为 self.exec_sql 的参数
        
        :params sql_name:
            根据 --sql.name 参数而来，同时也是 self.__sql_file<dict> 的 key，
            通过它来获取到对应的 sql 脚本内容 (即 self.__sql_file<dict> 的 value)
        """
        sqls = self.__sql_file[sql_name].split(';')
        if sqls:
            for sql in sqls:
                sql = sql.strip()
                if sql:
                    self.exec_sql(presto_cursor, sql)
                    print(presto_cursor.fetchall())


    def get_placeholder_loop(self):
        """
        获取 self.__placeholder_loop，self.__placeholder_loop 为 dict 类型，
        根据 --placeholder.loop 参数得来，形式为 {sql_name: placeholder}

        .. note:
            这里解释一下 placeholder 和 placeholder_loop 的区别，通过 --placeholder.sql 得到获取
            placeholder 的 sql 并执行后，我们会拿到 self.__placeholder，比如 {'max_id': '666666'}，
            假如我们没有提供 --placeholder.loop 参数，那么我们就会将 self.__placeholder['max_id'] 填到
            sql_name 对应的 sql 语句中，即 self.__sql_file[sql_name]，比如它语句里有这么一段:
            ```
            select * from xxx where id > {max_id}
            ```
            这时就会将 '666666' 替换掉 {max_id}:
            ```
            select * from xxx where id > '666666'
            ```
            假如我们获取到的 self.__placeholder = {'user_id': '1, 2, 3, 4'}，同时我们的参数为
            --sql.name fully-placeholder --placeholder.loop user_id:fully-placeholder
            那么这时就会将 1, 2, 3, 4 分别替换 fully-placeholder 中有 {user_id} 的位置，然后分别执行，
            即一个 placeholder 有多少个值，他就要轮流将值填进 sql 中，然后轮流执行，因此叫 loop.
        """
        placeholder_loop = {}

        if self.__args.placeholder_loop is not None:

            for placeholder_loop_sql in self.__args.placeholder_loop:

                placeholder = placeholder_loop_sql.split(':')[0]
                sql_name = placeholder_loop_sql.split(':')[1]

                placeholder_loop[sql_name] = placeholder

        self.__placeholder_loop = placeholder_loop


    def exec_placeholder_loop_sql(self, sql_name):
        separator = self.__args.placeholder_loop_value_separator
        placeholder_loop_values = self.__placeholders[self.__placeholder_loop[sql_name]].split(separator)

        for key in self.__placeholders:
            if key != self.__placeholder_loop[sql_name]:
                self.__sql_file[sql_name] = self.__sql_file[sql_name].replace('{' + key + '}', self.__placeholders[key])

        for lv in placeholder_loop_values:
            temp_sql_file = self.__sql_file[sql_name].replace('{' + self.__placeholder_loop[sql_name] + '}', lv)
            self.exec_sql_ignore_result(sql_name)


    def execute(self):
        with self.__get_presto_connection() as presto_connection:

            presto_cursor = presto_connection.cursor()

            self.get_placeholders(presto_cursor)
            self.get_placeholder_loop()
            self.get_sql_file()

            for sql_name in self.__sql_file.keys():
                if sql_name in self.__placeholder_loop.keys():
                    self.exec_placeholder_loop_sql(sql_name)
                else:
                    self.fill_placeholders(sql_name)
                    self.exec_sql_ignore_result(presto_cursor, sql_name)

        logging.info("============ Finish =============")

    
    def show_usage(self):
        if self.__args.usage is True:
            print(PrestoETL.USAGE)

    def test(self):
        print(self.args_dict)

if __name__ == '__main__':
    executor = PrestoETL()
    executor.execute()
    # executor.test()
