from pyhive import hive

conn = hive.connect(host='10.10.22.6', port=10000, username='hive', database='ods_crm')
cursor = conn.cursor()
cursor.execute('SELECT * FROM order_info LIMIT 10', async=True)
print cursor.fetchall()
