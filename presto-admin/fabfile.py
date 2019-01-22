import configparser
from fabric import Connection, SerialGroup
from invoke import task

# load config
config = configparser.ConfigParser()
config.read('config.ini')

# set coordinato
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
def uname(c):
    coordinator_group.run('uname -s')
    worker_group.run('uname -s')
    c.run('uname -s')


@task
def show_catalog(c):
    coordinator_group.run('ls -la ' + coordinator_catalog_path)
    worker_group.run('ls -la ' + worker_catalog_path)