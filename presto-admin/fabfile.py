import os
import logging
import configparser
from fabric import Connection, SerialGroup
from invoke import task

# load config
config = configparser.ConfigParser()
config.read('config.ini')

# set coordinator
coordinator_connections = []
coordinator_user = config['COORDINATOR'].pop('user')
coordinator_password = config['COORDINATOR'].pop('password')
coordinator_catalog_path = config['COORDINATOR'].pop('catalog_path')
for host_key in config['COORDINATOR'].keys():
    host = config['COORDINATOR'][host_key]
    conn = Connection(host=host, user=coordinator_user, connect_kwargs={'password': coordinator_password})
    coordinator_connections.append(conn)
coordinator_group = SerialGroup.from_connections(coordinator_connections)

# set worker
worker_connections = []
worker_user = config['WORKER'].pop('user')
worker_password = config['WORKER'].pop('password')
worker_catalog_path = config['WORKER'].pop('catalog_path')
for host_key in config['WORKER'].keys():
    host = config['WORKER'][host_key]
    conn = Connection(host=host, user=worker_user, connect_kwargs={'password': worker_password})
    worker_connections.append(conn)
worker_group = SerialGroup.from_connections(worker_connections)


@task
def backup(c, type):
    if type == 'catalog':
        logging.info("backup catalog..." + '='*60)
        if os.path.exists('catalog.bak'):
            os.rmdir('catalog.bak')
        c.run('cp -r catalog catalog.bak')
        logging.info("backup finish" + '='*60)


@task
def reload(c, type):
    if type == 'catalog':

        # remove catalog
        logging.info("remove coordinator catalog..." + '='*60)
        coordinator_group.run('rm -rf ' + coordinator_catalog_path + '/*')
        logging.info("remove finish" + '='*60)

        logging.info("remove worker catalog..." + '='*60)
        worker_group.run('rm -rf ' + worker_catalog_path + '/*')
        logging.info("remove finish" + '='*60)

        # put new catalog
        logging.info("put new catalog to coordinator..." + '='*60)
        for conn in coordinator_group:
            logging.info("[{}]:".format(conn.host))
            for pwd, sub_dir, files in os.walk('catalog'):
                for file in files:
                    conn.put('catalog/{}'.format(file), coordinator_catalog_path)
                break
        
        logging.info("put new catalog to worker..." + '='*60)
        for conn in worker_group:
            logging.info("[{}]:".format(conn.host))
            for pwd, sub_dir, files in os.walk('catalog'):
                for file in files:
                    conn.put('catalog/{}'.format(file), worker_catalog_path)
                break


@task
def uname(c):
    coordinator_group.run('uname -s')
    worker_group.run('uname -s')
    c.run('uname -s')


@task
def show(c, type):
    if type == 'catalog':
        coordinator_group.run('ls -la ' + coordinator_catalog_path)
        worker_group.run('ls -la ' + worker_catalog_path)