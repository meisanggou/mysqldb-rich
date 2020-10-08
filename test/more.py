#! /usr/bin/env python
# coding: utf-8
__author__ = '鹛桑够'

import pymysql
from mysqldb_rich.db2 import DB, RichDB

try:
    from dbutils.persistent_db import PersistentDB
    from dbutils.pooled_db import PooledDB
except ImportError:
    from DBUtils.PersistentDB import PersistentDB
    from DBUtils.PooledDB import PooledDB


g_pool = None


def pool(shareable, db_name):
    host = "127.0.01"
    port = 3306
    user = "dms"
    password = "gene_ac252"
    g_pool = None
    if g_pool is None:
        if shareable is True:
            g_pool = PooledDB(pymysql, host="127.0.01", port=3306, user="dms", passwd="gene_ac252", db=db_name,
                                      charset='utf8', blocking=1, maxconnections=3)
        else:
            g_pool = PersistentDB(pymysql, host=host, port=port, user=user, passwd=password, db=db_name,
                                          charset='utf8')

    conn = g_pool.connection()
    cursor = conn.cursor()
    return conn, cursor

db1 = RichDB("127.0.0.1", 3306, "dms", "gene_ac252", "dms")

db2 = RichDB("127.0.0.1", 3306, "dms", "gene_ac252", "information_schema")

conn1, cursor1 = pool(True, "dms")

db1.execute("select * from sys_user;")
cursor1.close()
conn1.close()


conn2, cursor2 = pool(True, "information_schema")
db2.execute("select * from TABLES")