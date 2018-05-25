#! /usr/bin/env python
# coding: utf-8

import json
import MySQLdb
import threading
from DBUtils.PersistentDB import PersistentDB
from DBUtils.PooledDB import PooledDB

__author__ = '鹛桑够'


thread_data = threading.local()


class SimpleDB(object):
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT = '%Y-%m-%d'

    _pool = None
    max_connections = 3

    def __init__(self, host, port, user, password, db_name):
        thread_data.conn = None
        thread_data.cursor = None
        self.host = host
        self._db_port = port
        self._db_user = user
        self._db_password = password
        self._db_name = db_name

    @staticmethod
    def _connect(host, port, user, password, db_name, shareable=True):
        if SimpleDB._pool is None:
            if shareable is True:
                SimpleDB._pool = PooledDB(MySQLdb, host=host, port=port, user=user, passwd=password, db=db_name,
                                          charset='utf8', blocking=1, maxconnections=SimpleDB.max_connections)
            else:
                SimpleDB._pool = PersistentDB(MySQLdb, host=host, port=port, user=user, passwd=password, db=db_name,
                                              charset='utf8')

        conn = SimpleDB._pool.connection()
        cursor = conn.cursor()
        return conn, cursor

    def connect(self):
        conn, cursor = self._connect(self.host, self._db_port, self._db_user, self._db_password, self._db_name)
        thread_data.conn = conn
        thread_data.cursor = cursor
        return conn, cursor

    def literal(self, s):
        if isinstance(s, basestring):
            pass
        elif isinstance(s, dict) or isinstance(s, tuple) or isinstance(s, list):
            s = json.dumps(s)
        elif isinstance(s, set):
            s = json.dumps(list(s))
        return s

    def execute(self, sql_query, args=None, freq=0, print_sql=False, w_literal=False, auto_close=True):
        if "cursor" not in thread_data.__dict__:
            thread_data.cursor = None
        if thread_data.cursor is None:
            self.connect()
        if args is not None and w_literal is False:
            if isinstance(args, (tuple, list)) is True:
                args = map(self.literal, args)
            elif isinstance(args, dict) is True:
                for k, v in args.items():
                    args[k] = self.literal(v)
        try:
            if print_sql is True:
                print(sql_query)
            handled_item = thread_data.cursor.execute(sql_query, args=args)
        except MySQLdb.Error as error:
            print(error)
            if freq >= 3 or error.args[0] in [1054, 1064, 1146, 1065, 1040]:  # 列不存在 sql错误 表不存在 empty_query too_many_connectons
                self.close()
                raise MySQLdb.Error(error)
            self.connect()
            return self.execute(sql_query=sql_query, args=args, freq=freq + 1, w_literal=True, auto_close=auto_close)
        if auto_close is True:
            self.close()
        return handled_item

    def fetchone(self):
        r = thread_data.cursor.fetchone()
        self.close()
        return r

    def fetchall(self):
        r = thread_data.cursor.fetchall()
        self.close()
        return r

    def close(self):
        if thread_data.cursor:
            thread_data.cursor.close()
        if thread_data.conn:
            thread_data.conn.close()
        thread_data.conn = None
        thread_data.cursor = None