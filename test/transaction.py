#! /usr/bin/env python
# coding: utf-8

import threading
from time import sleep
from functools import partial
from mysqldb_rich.db2 import DB

__author__ = '鹛桑够'

db = DB()
t = "sys_users"
cols = ["account", "password", "role"]


def special_print(prefix, s):
    prefix = "%s%s" % (prefix, s)
    print(prefix)


def read(index):
    _print = partial(special_print, "-----read-----%s-----" % index)
    for i in range(5):
        items = db.execute_select(t, cols=cols, where_value=dict(user_no=5872))
        _print(items[0]["role"])
        sleep(2)


def update(index):
    try:
        db.start_transaction()
        _print = partial(special_print, "-----update-----%s-----" % index)
        for i in range(5):
            _print("start select")
            items = db.execute_select_with_lock(t, cols=cols, where_value=dict(user_no=5872))
            role = items[0]["role"]
            _print(role)
            _print("start update")
            # if i % 3 == 0:
                # raise RuntimeError("except")
                # l = db.execute_update(t, update_value=dict(role2=role + 1), where_value=dict(user_no=5872))
            l = db.execute_update(t, update_value=dict(role=role + 1), where_value=dict(user_no=5872))
            # l = db.execute_update(t, update_value_list=["role=role+1"], where_value=dict(user_no=5872))
            _print("line %s" % l)
            # sleep(1)
        # sleep(10)
        db.end_transaction()
        # sleep(20)
    except Exception as e:
        print(e)
        db.end_transaction(fail=True)
        sleep(1)


if __name__ == "__main__":
    # t_read = threading.Thread(target=read, args=(1, ))
    # t_read.start()
    for i in range(500):
        t_update = threading.Thread(target=update, args=(i, ))
        t_update.start()
    sleep(200)