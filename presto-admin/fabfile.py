import os
import shutil
import logging
import coloredlogs, logging
import configparser
from fabric import Connection, SerialGroup
from invoke import task


# Create a logger object.
logger = logging.getLogger('presto-admin')
coloredlogs.install(level='INFO', logger=logger)


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
        if os.path.exists('catalog.bak'):
            shutil.rmtree('catalog.bak')
        c.run('cp -r catalog catalog.bak')


@task
def reload(c, type):
    if type == 'catalog':

        # remove catalog
        coordinator_group.run('rm -rf ' + coordinator_catalog_path + '/*')
        worker_group.run('rm -rf ' + worker_catalog_path + '/*')

        # put new catalog
        for conn in coordinator_group:
            for pwd, sub_dir, files in os.walk('catalog'):
                logger.info("[{}]: reloading...".format(conn.host))
                for file in files:
                    conn.put('catalog/{}'.format(file), coordinator_catalog_path)
                logger.info("[{}]: reload complete!".format(conn.host))
                break
        
        for conn in worker_group:
            for pwd, sub_dir, files in os.walk('catalog'):
                logger.info("[{}]: reloading...".format(conn.host))
                for file in files:
                    conn.put('catalog/{}'.format(file), worker_catalog_path)
                logger.info("[{}]: reload complete!".format(conn.host))
                break


@task
def show(c, type):
    if type == 'catalog':
        logger.info("list coordinator catalog file...")
        coordinator_group.run('ls -lah ' + coordinator_catalog_path)
        logger.info("list worker catalog file...")
        worker_group.run('ls -lah ' + worker_catalog_path)