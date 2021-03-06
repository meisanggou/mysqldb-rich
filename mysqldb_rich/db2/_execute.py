#! /usr/bin/env python
# coding: utf-8

from datetime import datetime, date
from ._db import SimpleDB

__author__ = '鹛桑够'


def merge_where(where_value=None, where_is_none=None, where_cond=None, where_cond_args=None, prefix_value=None):
    args = []
    if where_cond is None:
        where_cond = list()
    else:
        where_cond = list(where_cond)
        if isinstance(where_cond_args, (list, tuple)):
            args.extend(where_cond_args)
    if where_value is not None:
        where_args = dict(where_value).values()
        args.extend(where_args)
        for key in dict(where_value).keys():
            where_cond.append("%s=%%s" % key)
    if isinstance(prefix_value, dict) is True:
        for key in prefix_value.keys():
            v = "%s" % prefix_value[key]
            v = v.replace("_", r"\_").replace("%", r"\%")
            v = "%s%%" % v
            where_cond.append("%s LIKE %%s" % key)
            args.append(v)
    if where_is_none is not None and len(where_is_none) > 0:
        for key in where_is_none:
            where_cond.append("%s is NULL" % key)
    return where_cond, args


class SelectDB(SimpleDB):

    def execute_multi_select(self, table_name, where_value=None, cols=None, package=True, **kwargs):
        kwargs = dict(kwargs)
        if cols is None:
            select_item = "*"
        else:
            select_item = ",".join(tuple(cols))
        select_sql = "SELECT %s FROM %s" % (select_item, table_name)
        sql_query = ""
        args = []
        if isinstance(where_value, dict):
            for item in where_value:
                value_list = where_value[item]
                for value in value_list:
                    sql_query += "{0} WHERE {1}=%s union ".format(select_sql, item)
                    args.append(value)
        if len(sql_query) <= 0:
            return []
        sql_query = sql_query[:-7]
        order_by = kwargs.pop("order_by", None)
        order_desc = kwargs.pop("order_desc", False)
        if order_by is not None:
            if isinstance(order_by, list) or isinstance(order_by, tuple):
                sql_query += " ORDER BY %s" % ",".join(order_by)
            elif isinstance(order_by, str):
                sql_query += " ORDER BY %s" % order_by
            if order_desc is True:
                sql_query += " DESC"
        sql_query += ";"
        exec_result = self.execute(sql_query, args, auto_close=False)
        db_items = self.fetchall()
        if cols is not None and package is True:
            select_items = []
            for db_item in db_items:
                r_item = dict()
                for i in range(len(cols)):
                    c_v = db_item[i]
                    if isinstance(c_v, datetime):
                        c_v = c_v.strftime(self.TIME_FORMAT)
                    elif isinstance(c_v, date):
                        c_v = c_v.strftime(self.DATE_FORMAT)
                    elif isinstance(c_v, bytes):
                        if c_v == b"\x00":
                            c_v = 0
                        elif c_v == b"\x01":
                            c_v = 1
                    r_item[cols[i]] = c_v
                select_items.append(r_item)
            return select_items
        return db_items

    def execute_select(self, table_name, where_value=None, where_cond=None, cols=None, package=True, **kwargs):
        kwargs = dict(kwargs)
        where_is_none = kwargs.pop("where_is_none", None)
        where_cond_args = kwargs.pop("where_cond_args", None)
        prefix_value = kwargs.pop("prefix_value", None)
        update_lock = kwargs.pop("update_lock", False)
        where_cond, args = merge_where(where_value=where_value, where_cond=where_cond, where_is_none=where_is_none,
                                       where_cond_args=where_cond_args, prefix_value=prefix_value)
        if cols is None:
            select_item = "*"
        else:
            select_item = ",".join(tuple(cols))
        if len(where_cond) > 0:
            sql_query = "SELECT %s FROM %s WHERE %s" % (select_item, table_name, " AND ".join(where_cond))
        else:
            sql_query = "SELECT %s FROM %s" % (select_item, table_name)
        order_by = kwargs.pop("order_by", None)
        order_desc = kwargs.pop("order_desc", False)
        limit = kwargs.pop("limit", None)
        if order_by is not None:
            if isinstance(order_by, list) or isinstance(order_by, tuple):
                sql_query += " ORDER BY %s" % ",".join(order_by)
            elif isinstance(order_by, str) or isinstance(order_by, str):
                sql_query += " ORDER BY %s" % order_by
            if order_desc is True:
                sql_query += " DESC"
        if isinstance(limit, int):
            sql_query += " LIMIT %s" % limit
        if update_lock is True:
            sql_query += " FOR UPDATE"
        sql_query += ";"
        print_sql = kwargs.get("print_sql", False)
        exec_result = self.execute(sql_query, args, print_sql=print_sql, auto_close=False)
        db_items = self.fetchall()
        if cols is not None and package is True:
            select_items = []
            for db_item in db_items:
                r_item = dict()
                for i in range(len(cols)):
                    c_v = db_item[i]
                    if isinstance(c_v, datetime):
                        c_v = c_v.strftime(self.TIME_FORMAT)
                    elif isinstance(c_v, date):
                        c_v = c_v.strftime(self.DATE_FORMAT)
                    elif isinstance(c_v, bytes):
                        if c_v == b"\x00":
                            c_v = 0
                        elif c_v == b"\x01":
                            c_v = 1
                    r_item[cols[i]] = c_v
                select_items.append(r_item)
            return select_items
        return db_items

    def execute_select_with_lock(self, table_name, where_value=None, where_cond=None, cols=None, package=True, **kwargs):
        kwargs["update_lock"] = True
        return self.execute_select(table_name, where_value=where_value, where_cond=where_cond, cols=cols,
                                   package=package, **kwargs)

    def execute_select_left(self, b_t_name, j_t_name, join_key, **kwargs):
        where_is_none = kwargs.pop("where_is_none", None)
        where_value = kwargs.pop("where_value", {"1": 1})
        where_cond = kwargs.pop("where_cond", None)
        where_cond, args = merge_where(where_value=where_value, where_cond=where_cond, where_is_none=where_is_none)
        cols = list()
        package_keys = list()
        b_cols = kwargs.pop("b_cols", None)
        if b_cols is not None and isinstance(b_cols, list):
            for b_col in b_cols:
                cols.append("%s.%s" % (b_t_name, b_col))
                package_keys.append(b_col)
        j_cols = kwargs.pop("j_cols", None)
        if j_cols is not None and isinstance(j_cols, list):
            for j_col in j_cols:
                cols.append("%s.%s" % (j_t_name, j_col))
                package_keys.append(j_col)
        if len(cols) <= 0:
            s_item = "*"
        else:
            s_item = ",".join(cols)
        sql_query = "SELECT {0} FROM {1} LEFT JOIN {2} ON {1}.{3}={2}.{3}".format(s_item, b_t_name, j_t_name, join_key)
        if len(where_cond) > 0:
            sql_query += " WHERE %s" % " AND ".join(where_cond)
        order_by = kwargs.pop("order_by", None)
        order_desc = kwargs.pop("order_desc", False)
        limit = kwargs.pop("limit", None)
        if order_by is not None and (isinstance(order_by, list) or isinstance(order_by, tuple)):
            sql_query += " ORDER BY %s" % ",".join(order_by)
            if order_desc is True:
                sql_query += " DESC"
        if isinstance(limit, int):
            sql_query += " LIMIT %s" % limit
        sql_query += ";"
        exec_result = self.execute(sql_query, args, auto_close=False)
        db_items = self.fetchall()
        if len(package_keys) > 0:
            select_items = []
            for db_item in db_items:
                r_item = dict()
                for i in range(len(package_keys)):
                    c_v = db_item[i]
                    if isinstance(c_v, datetime):
                        c_v = c_v.strftime(self.TIME_FORMAT)
                    elif isinstance(c_v, date):
                        c_v = c_v.strftime(self.DATE_FORMAT)
                    elif isinstance(c_v, bytes):
                        if c_v == b"\x00":
                            c_v = 0
                        elif c_v == b"\x01":
                            c_v = 1
                    r_item[package_keys[i]] = c_v
                select_items.append(r_item)
            return select_items
        return db_items


class InsertDB(SimpleDB):

    def execute_insert(self, table_name, kwargs, ignore=False):
        keys = dict(kwargs).keys()
        if ignore is True:
            sql_query = "INSERT IGNORE INTO %s (%s) VALUES (%%(%s)s);" % (table_name, ",".join(keys),
                                                                          ")s,%(".join(keys))
        else:
            sql_query = "INSERT INTO %s (%s) VALUES (%%(%s)s);" % (table_name, ",".join(keys), ")s,%(".join(keys))
        return self.execute(sql_query, args=kwargs, auto_close=True)

    def execute_duplicate_insert(self, t_name, kwargs, u_keys=None, p1_keys=None, **options):
        if isinstance(kwargs, dict) is False:
            raise TypeError()
        u_v = options.pop('u_v', list())
        plus_keys = options.pop('plus_keys', list())
        if isinstance(u_v, list) is False:
            raise TypeError()
        keys = kwargs.keys()
        if isinstance(u_keys, (tuple, list)) is True:
            u_v.extend(map(lambda x: "{0}=VALUES({0})".format(x), u_keys))
        if isinstance(p1_keys, (tuple, list)) is True:
            u_v.extend(map(lambda x: "{0}={0}+1".format(x), p1_keys))
        if isinstance(plus_keys, (tuple, list)):
            u_v.extend(map(lambda x: "{0}={0}+VALUES({0})".format(x),
                           set(plus_keys)))
        if len(u_v) <= 0:
            return self.execute_insert(t_name, kwargs)
        sql = "INSERT INTO %s (%s) VALUES (%%(%s)s) ON DUPLICATE KEY UPDATE %s;" \
              % (t_name, ",".join(keys), ")s,%(".join(keys), ",".join(u_v))
        return self.execute(sql, args=kwargs, auto_close=True)

    def execute_insert_mul(self, table_name, cols, value_list, ignore=False):
        keys = cols
        if ignore is True:
            sql_query = "INSERT IGNORE INTO %s (%s) VALUES " % (table_name, ",".join(keys))
        else:
            sql_query = "INSERT INTO %s (%s) VALUES " % (table_name, ",".join(keys))
        if not isinstance(value_list, list):
            return -1
        if len(value_list) <= 0:
            return 0
        args = []
        for value_item in value_list:
            sql_query += "(" + ("%s," * len(value_item)).rstrip(",") + "),"
            args.extend(value_item)
        sql_query = sql_query.rstrip(",") + ";"
        return self.execute(sql_query, args=args, auto_close=True)

    def execute_duplicate_insert_mul(self, t_name, cols, values, u_keys=None, p1_keys=None, concat_keys=None, u_v=None,
                                     print_sql=False):
        u_l = list()
        if isinstance(u_v, (tuple, list)) is True:
            u_l.extend(u_v)
        if isinstance(u_keys, (tuple, list)) is True:
            u_l.extend(map(lambda x: "{0}=VALUES({0})".format(x), u_keys))
        if isinstance(p1_keys, (tuple, list)) is True:
            u_l.extend(map(lambda x: "{0}={0}+1".format(x), p1_keys))
        if isinstance(concat_keys, (tuple, list)) is True:
            u_l.extend(map(lambda x: "{0}=concat({0}, ',', VALUES({0}))".format(x), concat_keys))
        if len(u_l) <= 0:
            return self.execute_insert_mul(t_name, cols, values)
        keys = cols
        sql_query = "INSERT INTO %s (%s) VALUES " % (t_name, ",".join(keys))
        if isinstance(values, (list, tuple)) is False:
            raise TypeError()
        if len(values) <= 0:
            return 0
        args = []
        for value_item in values:
            sql_query += "(" + ("%s," * len(value_item)).rstrip(",") + "),"
            args.extend(value_item)
        sql_query = sql_query.rstrip(",") + " ON DUPLICATE KEY UPDATE %s;" % ",".join(u_l)
        return self.execute(sql_query, args=args, print_sql=print_sql, auto_close=True)


class UpdateDB(SimpleDB):

    def execute_update(self, table_name, update_value=None, update_value_list=None, where_value=None, where_is_none=None,
                       where_cond=None):
        if update_value_list is None:
            update_value_list = list()
        else:
            update_value_list = list(update_value_list)
        args = []
        if update_value is not None and isinstance(update_value, dict):
            args.extend(update_value.values())
            for key in update_value.keys():
                update_value_list.append("{0}=%s".format(key))
        if len(update_value_list) <= 0:
            return 0
        sql_query = "UPDATE %s SET %s WHERE " % (table_name, ",".join(update_value_list))
        if isinstance(where_cond, tuple) or isinstance(where_cond, list):
            where_cond = list(where_cond)
        else:
            where_cond = []
        if where_value is not None:
            where_args = dict(where_value).values()
            args.extend(where_args)
            for key in dict(where_value).keys():
                where_cond.append("%s=%%s" % key)
        if isinstance(where_is_none, (list, tuple)) and len(where_is_none) > 0:
            for key in where_is_none:
                where_cond.append("%s is NULL" % key)
        sql_query += " AND ".join(where_cond) + ";"
        return self.execute(sql_query, args=args, auto_close=True)

    def execute_plus(self, table_name, *args, **kwargs):
        where_value = kwargs.pop("where_value", None)
        where_is_none = kwargs.pop("where_is_none", None)
        where_cond = kwargs.pop("where_cond", None)

        update_value_list = []
        for item in args:
            update_value_list.append("{0}={0}+1".format(item))
        for key in kwargs:
            update_value_list.append("{0}={0}+{1}".format(key, kwargs[key]))
        return self.execute_update(table_name, update_value_list=update_value_list, where_value=where_value,
                                   where_is_none=where_is_none, where_cond=where_cond)

    def execute_logic_or(self, table_name, where_value=None, where_is_none=None, where_cond=None, **kwargs):
        update_value_list = []
        for key in kwargs:
            update_value_list.append("{0}={0}|{1}".format(key, kwargs[key]))
        return self.execute_update(table_name, update_value_list=update_value_list, where_value=where_value,
                                   where_is_none=where_is_none, where_cond=where_cond)

    def execute_logic_non(self, table_name, where_value=None, where_is_none=None, where_cond=None, **kwargs):
        update_value_list = []
        for key in kwargs:
            update_value_list.append("{0}={0}&~{1}".format(key, kwargs[key]))
        return self.execute_update(table_name, update_value_list=update_value_list, where_value=where_value,
                                   where_is_none=where_is_none, where_cond=where_cond)


class DeleteDB(SimpleDB):

    def execute_delete(self, table_name, where_value=None, where_cond=None):
        where_cond, args = merge_where(where_value=where_value, where_cond=where_cond)
        if len(where_cond) <= 0:
            return 0
        sql_query = "DELETE FROM %s WHERE %s;" % (table_name,  " AND ".join(where_cond))
        return self.execute(sql_query, args, auto_close=True)
