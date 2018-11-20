import os
import sys
import argparse
import logging
import prestodb
import pymysql
import requests
import json
import time


sql = """
    select array[1, 2] AS count, array['beijing', 'guangzhou' AS cities
"""

cur = prestodb.dbapi.connect(
    host='10.10.22.5',
    port=10300,
    user='dev',
    catalog='dev_hive'
).cursor()

r = cur.fetchall(sql)

print(r.)