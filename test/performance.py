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


if __name__ == "__main__":
    freq = 1000
    test_select(freq)
    freq *= 10
    test_select(freq)
    freq *= 10
    test_select(freq)
    # freq *= 10
    # test_select(freq)
    # freq *= 10
    # test_select(freq)