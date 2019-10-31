#! /usr/bin/env python
# coding: utf-8

import time
from time import sleep
from threading import Thread
from mysqldb_rich.db2 import DB

__author__ = '鹛桑够'

db = DB(conf_path="D:/Project/dms/mysql_app.conf")
# db.connect()

t = "sys_user"
cols = ["user_name", "password"]


def test_select(t_name, freq=1):
    print("------------------------test %s freq=%s-----------------------" % (t_name, freq))
    start_time = time.time()
    print("%s start time %s" % (t_name, start_time))
    # import requests
    # url = "http://127.0.0.1:6001/right/sample/"
    # method = "POST"
    # headers = {"Content-Type":"application/json"}
    # data = {"account":"zh_test","sample_no":3701,"portal":1,"project_no":111}
    # resp = requests.request(method, url, headers=headers, json=data)
    # assert resp.status_code == 200
    # r_data = resp.json()
    # print "success" if r_data["status"] % 10000 < 100 else "status exception"
    # print(r_data["status"])
    # print(r_data["message"])
    # print r_data["data"] if "data" in r_data else "no data"
    for i in range(freq):
        items = db.execute_select(t, cols=cols, package=True)
        print(items)
    end_time = time.time()
    use_time = end_time - start_time
    sleep(2)
    print("%s end time %s" % (t_name, end_time))
    print("%suse time %s" % (t_name, use_time))


if __name__ == "__main__":
    for i in range(5):
        t_t = Thread(target=test_select, args=(i, 10))
        t_t.daemon = True
        t_t.start()

    sleep(1000)