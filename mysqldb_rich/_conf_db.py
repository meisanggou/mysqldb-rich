#! /usr/bin/env python
# coding: utf-8

import os
import ConfigParser
from _db import SimpleDB

__author__ = '鹛桑够'


class ConfDB(SimpleDB):
    conf_name = "mysql_app.conf"
    conf_path_environ_key = "DB_CONF_PATH"

    def __init__(self, conf_path=None, conf_dir=None, readonly=False, user=None, password=None):
        self.readonly = readonly
        if conf_path is None:
            if conf_dir is not None:
                conf_path = os.path.join(conf_dir, self.conf_name)
            elif os.environ.get(self.conf_path_environ_key) is not None:
                conf_path = os.environ.get(self.conf_path_environ_key)
            else:
                conf_path = self.conf_name
        self.conf_path = conf_path
        o = self._read_conf(conf_path, readonly)
        if user is not None and password is not None:
            o["user"] = user
            o["password"] = password
        SimpleDB.__init__(self, **o)

    @staticmethod
    def _read_conf(conf_path, readonly):
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        basic_section = "db_basic"
        host = config.get(basic_section, "host")
        db_name = config.get(basic_section, "name")
        db_port = config.getint(basic_section, "port")
        if readonly is True:
            user_section = "%s_read_user" % basic_section
        else:
            user_section = "%s_user" % basic_section
        db_user = config.get(user_section, "user")
        db_password = config.get(user_section, "password")
        return dict(host=host, db_name=db_name, port=db_port, user=db_user, password=db_password)
