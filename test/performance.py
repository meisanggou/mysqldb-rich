#! /usr/bin/env python
# coding: utf-8

import time
from mysqldb_rich.db2 import DB

__author__ = '鹛桑够'


db = DB()
t = "sys_users"
cols = ["account", "password"]


def test_select(freq=1):
    print("------------------------test freq=%s-----------------------" % freq)
    start_time = time.time()
    print("start time %s" % start_time)
    for i in range(freq):
        db.connect()
        items = db.execute_select(t, cols=cols)
        db.close()
        # print(items)
    end_time = time.time()
    use_time = end_time - start_time
    print("end time %s" % end_time)
    print("use time %s" % use_time)


def test_insert(freq=1):
    for i in range(freq):
        account = "TF%s%s" % (int(time.time()), i)
        kwargs = dict(account=account)
        l = db.execute_insert(t, kwargs=kwargs, ignore=True)
        print(l)


if __name__ == "__main__":
    freq = 10
    test_insert(freq)
    # freq *= 10
    # test_select(freq)
    # freq *= 10
    # test_select(freq)
    # freq *= 10
    # test_select(freq)
    # freq *= 10
    # test_select(freq)