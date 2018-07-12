import sys
import argparse
import logging
import prestodb
import pymysql
import requests
import json


class Presqoop():
    """
    A Sqoop like tools to import/export datas by using presto

    **Basic**
    
    基于 presto 做的数据导入/导出脚本，功能仿照 sqoop 设计，尽量实现 sqoop 的功能

    .. version v1.0
    """

    # 执行脚本必须要带的参数
    NECESSARY_ARGS = {
        '--presto-host': 'presto_host',
        '--presto-port': 'presto_port',
        '--presto-user': 'presto_user',
        '--presto-catalog': 'presto_catalog',
        '--presto-schema': 'presto_schema',
        '--table': 'table'
    }


    def __init__(self):
        """
        初始化时将参数通过 self.__set_args() 绑定到 self.__args 变量上
        同时初始化 session 以作为请求 url 的会话
        """
        self.__args = self.__set_args()
        self.__check_args()
        

    def __set_args(self):
        """
        设置参数选项
        """
        parser = argparse.ArgumentParser()

        parser.add_argument('execute_type')

        # set connection arguments
        parser.add_argument('--presto-host', action='store', dest='presto_port',type=str, help="set presto port")
        parser.add_argument('--presto-port', action='store', dest='presto_port',type=int, help="set presto port")
        parser.add_argument('--presto-user', action='store', dest='presto_user', type=str, help="set presto user")
        parser.add_argument(
            '--presto-catalog', action='store', dest='presto_catalog', type=str, help="set presto catalog"
        )
        parser.add_argument('--presto-schema', action='store', dest='presto_schema', type=str, help="set presto schema")
        parser.add_argument('--tabel', action='store', nargs='*', dest='table', type=str, help="set table names")

        # set log arguments
        parser.add_argument('--log-path', action='store', dest='log_path', type=str, help="set log path")

        # set config arguments
        parser.add_argument('-l', '--list', action='store_true', dest='config_list', defalut=False, help="list config")
        parser.add_argument('--presto', action='store', dest='config_presto', type=str, help="set presto config name")
        parser.add_argument('--log', action='store', dest='config_log', type=str, help="set log config name")

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
        # check necesary arguments
        for necessary_arg in SqlFlowExecutor.NECESSARY_ARGS.values():
            if self.__args_dict[necessary_arg] is None:
                logging.error(
                    "Please provide all necessary arguments: {}".format(SqlFlowExecutor.NECESSARY_ARGS.keys())
                )
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


    def __get_presto_connection(self):
        return prestodb.dbapi.connect(
            host=self.__args.preto_host,
            port=self.__args.presto_port,
            user=self.__args.presto_user,
            catalog=self.__args.presto_catalog,
            schema=self.__args.presto_schema,
        )


    # TODO: 2018-07-13
    # 对各种数据源与 presto 支持的 DATATYPE 进行调研，
    # 考察相互导入导出的类型转换问题，再进行功能函数的编写
