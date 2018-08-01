#! /usr/bin/env python
# coding: utf-8

import os
from _conf_db import ConfDB
from _execute import SimpleDB, SelectDB, InsertDB, UpdateDB, DeleteDB


__author__ = '鹛桑够'


class RichDB(SelectDB, InsertDB, UpdateDB, DeleteDB):

    def __init__(self, host, port, user, password, db_name):
        SimpleDB.__init__(self, host, port, user, password, db_name)


class DB(ConfDB, SelectDB, InsertDB, UpdateDB, DeleteDB):
    t_tables = "information_schema.TABLES"

    def __init__(self, conf_path=None, conf_dir=None, readonly=False, user=None, password=None):
        ConfDB.__init__(self, conf_path, conf_dir, readonly, user, password)

    def execute_call(self, p_name, *args):
        sql_query = "CALL %s(" % p_name
        sql_query += ",".join(map(self.literal, args))
        sql_query += ");"
        return self.execute(sql_query, auto_close=True)

    def table_exist(self, t_name):
        where_value = dict(TABLE_SCHEMA=self._db_name, TABLE_TYPE='BASE TABLE', TABLE_NAME=t_name)
        cols = ["TABLE_NAME", "CREATE_TIME", "TABLE_COMMENT"]
        l = self.execute_select(self.t_tables, where_value=where_value, cols=cols, package=False)
        if l == 0:
            return False
        return True

    def create_user(self, user, password, host='localhost', db=None, readonly=False):
        l = self.execute_select("mysql.user", where_value=dict(user=user, host=host))
        if l <= 0:
            c_sql = "CREATE USER %s@%s IDENTIFIED BY %s;"
            self.execute(c_sql, args=(user, host, password), auto_close=True)
        if db is not None:
            if readonly is False:
                g_sql = "GRANT ALL ON {db}.* TO %s@%s;"
            else:
                g_sql = "GRANT SELECT ON {db}.* TO %s@%s;"
            self.execute(g_sql.format(db=db), args=(user, host), auto_close=True)
        return True

    def root_init_conf(self, host='localhost'):
        o = self._read_conf(self.conf_path, False)
        self.create_user(o["user"], o["password"], host=host, db=o["db_name"], readonly=False)
        o = self._read_conf(self.conf_path, True)
        self.create_user(o["user"], o["password"], host=host, db=o["db_name"], readonly=True)

    def source_file(self, file_path):
        cmd = "mysql -u%s -p%s %s < %s" % (self._db_user, self._db_password, self._db_name, file_path)
        os.system(cmd)

    def link(self):
        cmd = "mysql -h%s -P%s -u%s -p'%s' %s" % (self.host, self._db_port, self._db_user, self._db_password,
                                                  self._db_name)
        os.system(cmd)
