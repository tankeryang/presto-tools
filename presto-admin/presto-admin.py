import os
import sys
import argparse
import logging
import textwrap


logging.basicConfig(level=logging.DEBUG)


class PrestoAdmin():
    """
    Presto 管理工具
    """

    USAGE = """
        python prestoetl.py <option> [arguments]

        for help
        --------
        python presto-admin.py -h

        example
        -------
        python presto-admin.py --usage: show usage
    """


    def __init__(self):
        """
        初始化时将参数通过 self.__set_args() 绑定到 self.__args 变量上
        """
        self.__args = self.__set_args()
        self.__check_args()


    def __set_args(self):
        """
        设置参数选项
        """
        parser = argparse.ArgumentParser(prog="python3 presto-admin.py", description="This is a python manage script")

        parser.add_argument('--usage', action='store_true', dest='usage', default=False, help="show usage")
        parser.add_argument(
            '--backup-catalog', action='store_true', dest='backup_catalog', default=False, help="backup catalog"
        )
        parser.add_argument(
            '--reload-catalog', action='store_true', dest='reload_catalog', default=False, help="reload catalog"
        )
        parser.add_argument(
            '--list-catalog', action='store_true', dest='list_catalog', default=False,
            help="list catalog file"
        )

        # # set presto coordinator hosts, password and catalog path
        # parser.add_argument(
        #     '--coordinator-hosts', action='store', dest='coordinator_hosts', nargs='*',
        #     help="set presto coordinator hosts"
        # )
        # parser.add_argument(
        #     '--coordinator-password', action='store', dest='coordinator_password',
        #     help="set presto coordinator password"
        # )
        # parser.add_argument(
        #     '--coordinator-catalog-path', action='store', dest='coordinator_catalog_path',
        #     help="set coordinator presto catalog path (e.g. /etc/ecm/presto-conf/catalog)"
        # )

        # # set presto worker hosts and password
        # parser.add_argument(
        #     '--worker-hosts', action='store', dest='worker_hosts', nargs='*',
        #     help="set presto worker hosts"
        # )
        # parser.add_argument(
        #     '--worker-password', action='store', dest='worker_password',
        #     help="set presto worker password"
        # )
        # parser.add_argument(
        #     '--worker-catalog-path', action='store', dest='worker_catalog_path',
        #     help="set worker presto catalog path (e.g. /etc/ecm/presto-conf/catalog)"
        # )

        args = parser.parse_args()

        # set args_dict
        args_key = list(map(lambda kv: kv[0], args._get_kwargs()))
        args_value = list(map(lambda kv: kv[1], args._get_kwargs()))

        self.__args_dict = dict(zip(args_key, args_value))

        return args


    def __check_args(self):
            self.list_catalog()
            self.backup_catalog()
            self.reload_catalog()
            self.show_usage()


    def backup_catalog(self):
        """
        backup catalog file
        """
        if self.__args.backup_catalog is True:
            logging.info("backuping catalog file...")
            os.system('fab backup catalog')
            logging.info("backuping complete!")


    def reload_catalog(self):
        """
        reload catalog file
        """
        if self.__args.reload_catalog is True:
            logging.info("reloading catalog file...")
            os.system('fab reload catalog')
            logging.info("reloading complete!")

    
    def list_catalog(self):
        if self.__args.list_catalog is True:
            logging.info("list catalog file...")
            os.system('fab show catalog')
            logging.info("list complete!")


    def show_usage(self):
        if self.__args.usage is True:
            print(textwrap.dedent(PrestoAdmin.USAGE))


if __name__ == '__main__':
    presto_admin = PrestoAdmin()
