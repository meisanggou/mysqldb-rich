#! /usr/bin/env python
# coding: utf-8

from datetime import datetime, date
from _db import SimpleDB

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
        sql_query = sql_query[:-7]
        order_by = kwargs.pop("order_by", None)
        order_desc = kwargs.pop("order_desc", False)
        if order_by is not None:
            if isinstance(order_by, list) or isinstance(order_by, tuple):
                sql_query += " ORDER BY %s" % ",".join(order_by)
            elif isinstance(order_by, unicode) or isinstance(order_by, str):
                sql_query += " ORDER BY %s" % order_by
            if order_desc is True:
                sql_query += " DESC"
        sql_query += ";"
        exec_result = self.execute(sql_query, args)
        if cols is not None and package is True:
            db_items = self.fetchall()
            select_items = []
            for db_item in db_items:
                r_item = dict()
                for i in range(len(cols)):
                    c_v = db_item[i]
                    if isinstance(c_v, datetime):
                        c_v = c_v.strftime(self.TIME_FORMAT)
                    elif isinstance(c_v, date):
                        c_v = c_v.strftime(self.DATE_FORMAT)
                    elif isinstance(c_v, str):
                        if c_v == "\x00":
                            c_v = False
                        elif c_v == "\x01":
                            c_v = True
                        else:
                            print(c_v)
                    r_item[cols[i]] = c_v
                select_items.append(r_item)
            return select_items
        return exec_result

    def execute_select(self, table_name, where_value=None, where_cond=None, cols=None, package=True, **kwargs):
        kwargs = dict(kwargs)
        where_is_none = kwargs.pop("where_is_none", None)
        where_cond_args = kwargs.pop("where_cond_args", None)
        prefix_value = kwargs.pop("prefix_value", None)
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
            elif isinstance(order_by, unicode) or isinstance(order_by, str):
                sql_query += " ORDER BY %s" % order_by
            if order_desc is True:
                sql_query += " DESC"
        if isinstance(limit, int):
            sql_query += " LIMIT %s" % limit
        sql_query += ";"
        print_sql = kwargs.get("print_sql", False)
        exec_result = self.execute(sql_query, args, print_sql=print_sql)
        if cols is not None and package is True:
            db_items = self.fetchall()
            select_items = []
            for db_item in db_items:
                r_item = dict()
                for i in range(len(cols)):
                    c_v = db_item[i]
                    if isinstance(c_v, datetime):
                        c_v = c_v.strftime(self.TIME_FORMAT)
                    elif isinstance(c_v, date):
                        c_v = c_v.strftime(self.DATE_FORMAT)
                    elif isinstance(c_v, str):
                        if c_v == "\x00":
                            c_v = False
                        elif c_v == "\x01":
                            c_v = True
                        else:
                            print(c_v)
                    r_item[cols[i]] = c_v
                select_items.append(r_item)
            return select_items
        return exec_result


class InsertDB(SimpleDB):

    def execute_insert(self, table_name, kwargs, ignore=False):
        keys = dict(kwargs).keys()
        if ignore is True:
            sql_query = "INSERT IGNORE INTO %s (%s) VALUES (%%(%s)s);" % (table_name, ",".join(keys),
                                                                          ")s,%(".join(keys))
        else:
            sql_query = "INSERT INTO %s (%s) VALUES (%%(%s)s);" % (table_name, ",".join(keys), ")s,%(".join(keys))
        return self.execute(sql_query, args=kwargs)

    def execute_duplicate_insert(self, t_name, kwargs, u_keys=None, p1_keys=None, u_v=None):
        if isinstance(kwargs, dict) is False:
            raise TypeError()
        if u_v is None:
            u_v = []
        if isinstance(u_v, list) is False:
            raise TypeError()
        keys = kwargs.keys()
        if isinstance(u_keys, (tuple, list)) is True:
            u_v.extend(map(lambda x: "{0}=VALUES({0})".format(x), u_keys))
        if isinstance(p1_keys, (tuple, list)) is True:
            u_v.extend(map(lambda x: "{0}={0}+1".format(x), p1_keys))
        if len(u_v) <= 0:
            return self.execute_insert(t_name, kwargs)
        sql = "INSERT INTO %s (%s) VALUES (%%(%s)s) ON DUPLICATE KEY UPDATE %s;" \
              % (t_name, ",".join(keys), ")s,%(".join(keys), ",".join(u_v))
        return self.execute(sql, args=kwargs)

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
        return self.execute(sql_query, args=args)

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
        return self.execute(sql_query, args=args, print_sql=print_sql)


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
        return self.execute(sql_query, args=args)

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
            update_value_list.append("{0}={0}&~1}".format(key, kwargs[key]))
        return self.execute_update(table_name, update_value_list=update_value_list, where_value=where_value,
                                   where_is_none=where_is_none, where_cond=where_cond)


class DeleteDB(SimpleDB):

    def execute_delete(self, table_name, where_value=None, where_cond=None):
        where_cond, args = merge_where(where_value=where_value, where_cond=where_cond)
        if len(where_cond) <= 0:
            return 0
        sql_query = "DELETE FROM %s WHERE %s;" % (table_name,  " AND ".join(where_cond))
        return self.execute(sql_query, args)