#! /usr/bin/env python
# coding: utf-8

import json
import MySQLdb

__author__ = '鹛桑够'


class SimpleDB(object):
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, host, port, user, password, db_name):
        self.conn = None
        self.cursor = None
        self.host = host
        self._db_port = port
        self._db_user = user
        self._db_password = password
        self._db_name = db_name

    @staticmethod
    def _connect(host, port, user, password, db_name):
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db_name, charset='utf8')
        cursor = conn.cursor()
        conn.autocommit(True)
        return conn, cursor

    def connect(self):
        self.conn, self.cursor = self._connect(self.host, self._db_port, self._db_user, self._db_password,
                                               self._db_name)

    def literal(self, s):
        if not self.conn:
            self.connect()
        if isinstance(s, basestring):
            pass
        elif isinstance(s, dict) or isinstance(s, tuple) or isinstance(s, list):
            s = json.dumps(s)
        elif isinstance(s, set):
            s = json.dumps(list(s))
        s = self.conn.literal(s)
        return s

    def execute(self, sql_query, args=None, freq=0, print_sql=False):
        if self.cursor is None:
            self.connect()
        if isinstance(sql_query, unicode):
            sql_query = sql_query.encode(self.conn.unicode_literal.charset)
        if args is not None:
            if isinstance(args, dict):
                sql_query = sql_query % dict((key, self.literal(item)) for key, item in args.iteritems())
            else:
                sql_query = sql_query % tuple([self.literal(item) for item in args])
        try:
            if print_sql is True:
                print(sql_query)
            handled_item = self.cursor.execute(sql_query)
        except MySQLdb.Error as error:
            print(error)
            if freq >= 3 or error.args[0] in [1054, 1064, 1146, 1065]:  # 列不存在 sql错误 表不存在 empty_query
                raise MySQLdb.Error(error)
            self.connect()
            return self.execute(sql_query=sql_query, freq=freq + 1)
        return handled_item

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        if self.cursor:
            self.cursor.close()
        self.conn.close()