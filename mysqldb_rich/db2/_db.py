#! /usr/bin/env python
# coding: utf-8

import json
import pymysql
import threading
try:
    from dbutils.persistent_db import PersistentDB
    from dbutils.pooled_db import PooledDB
except ImportError:
    from DBUtils.PersistentDB import PersistentDB
    from DBUtils.PooledDB import PooledDB

__author__ = '鹛桑够'


class SimpleDB(object):
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT = '%Y-%m-%d'
    AUTO_CLOSE = False

    _pool = dict()
    max_connections = 3

    def __init__(self, host, port, user, password, db_name, charset="utf8", use_unicode=None):
        self.thread_data = threading.local()
        self.thread_data.conn = None
        self.thread_data.cursor = None
        self.host = host
        self._db_port = port
        self._db_user = user
        self._db_password = password
        self._db_name = db_name
        if charset == "":
            charset = None
        self._connect_charset = charset
        self._use_unicode = use_unicode

    @staticmethod
    def _connect(host, port, user, password, db_name, connect_charset=None, use_unicode=None, shareable=True):
        _pool_id = "%s_%s_%s_%s" % (host, port, user, db_name)
        if _pool_id not in SimpleDB._pool:
            comm_kwargs = dict(host=host, port=port, user=user, passwd=password, db=db_name)
            if connect_charset is not None:
                comm_kwargs["charset"] = connect_charset
            if use_unicode is not None:
                comm_kwargs["use_unicode"] = use_unicode
            if shareable is True:
                pool = PooledDB(pymysql, blocking=1, maxconnections=SimpleDB.max_connections, **comm_kwargs)
            else:
                pool = PersistentDB(pymysql, **comm_kwargs)
            SimpleDB._pool[_pool_id] = pool

        conn = SimpleDB._pool[_pool_id].connection()
        cursor = conn.cursor()
        return conn, cursor

    def connect(self):
        conn, cursor = self._connect(self.host, self._db_port, self._db_user, self._db_password, self._db_name,
                                     self._connect_charset, self._use_unicode)
        self.thread_data.conn = conn
        self.thread_data.cursor = cursor
        return conn, cursor

    def literal(self, s):
        if isinstance(s, str):
            pass
        elif isinstance(s, dict) or isinstance(s, tuple) or isinstance(s, list):
            s = json.dumps(s)
        elif isinstance(s, set):
            s = json.dumps(list(s))
        return s

    @property
    def current_transaction(self):
        if "exec_transaction" not in self.thread_data.__dict__:
            self.thread_data.exec_transaction = None
        if self.thread_data.exec_transaction is not True:
            return False
        return True

    @current_transaction.setter
    def current_transaction(self, value):
        self.thread_data.exec_transaction = value

    def start_transaction(self):
        if self.current_transaction is False:
            self.current_transaction = True

    def end_transaction(self, **kwargs):
        fail = kwargs.pop("fail", False)
        if fail is True and self.thread_data.conn:
            self.thread_data.conn.rollback()
        self.current_transaction = False
        self.close()

    def execute(self, sql_query, args=None, freq=0, print_sql=False, w_literal=False, auto_close=AUTO_CLOSE):
        if "cursor" not in self.thread_data.__dict__:
            self.thread_data.cursor = None
        if self.thread_data.cursor is None:
            self.connect()
        if not args:
            args = None
        if args is not None and w_literal is False:
            if isinstance(args, (tuple, list)) is True:
                args = list(map(self.literal, args))
            elif isinstance(args, dict) is True:
                for k, v in args.items():
                    args[k] = self.literal(v)
        try:
            if print_sql is True:
                print(sql_query)
            handled_item = self.thread_data.cursor.execute(sql_query, args=args)
        except pymysql.Error as error:
            if self.current_transaction is True:
                self.end_transaction(fail=True)
                self.close()
                raise pymysql.Error(error)
            print(error)
            if freq >= 3 or error.args[0] in [1054, 1064, 1146, 1065, 1040]:  # 列不存在 sql错误 表不存在 empty_query too_many_connectons
                raise pymysql.Error(error)
            self.connect()
            return self.execute(sql_query=sql_query, args=args, freq=freq + 1, w_literal=True, auto_close=auto_close)
        if auto_close is True and self.current_transaction is False:
            self.close()
        elif self.current_transaction is False:
            self.thread_data.conn.commit()
        return handled_item

    def fetchone(self):
        r = self.thread_data.cursor.fetchone()
        if self.current_transaction is False:
            self.close()
        return r

    def fetchall(self):
        r = self.thread_data.cursor.fetchall()
        if self.current_transaction is False:
            self.close()
        return r

    def close(self):
        if self.thread_data.cursor:
            self.thread_data.cursor.close()
        if self.thread_data.conn:
            self.thread_data.conn.commit()
            self.thread_data.conn.close()
        self.thread_data.conn = None
        self.thread_data.cursor = None

