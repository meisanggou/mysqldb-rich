#! /usr/bin/env python
# coding: utf-8

import os
import sys
import json
from _conf_db import ConfDB
from _execute import SelectDB

__author__ = '鹛桑够'


class TableDB(ConfDB, SelectDB):
    t_tables = "information_schema.TABLES"
    t_columns = "information_schema.columns"

    def __init__(self, conf_path=None, conf_dir=None, readonly=False, user=None, password=None):
        ConfDB.__init__(self, conf_path, conf_dir, readonly, user, password)

    def table_exist(self, t_name):
        where_value = dict(TABLE_SCHEMA=self._db_name, TABLE_TYPE='BASE TABLE', TABLE_NAME=t_name)
        cols = ["TABLE_NAME", "CREATE_TIME", "TABLE_COMMENT"]
        l = self.execute_select(self.t_tables, where_value=where_value, cols=cols, package=False)
        if l == 0:
            return False
        return True

    def get_table_info(self, table_name):
        cols = ["column_name",  "column_type", "column_key", "column_default", "extra", "column_comment", "is_nullable"]
        where_value = dict(table_name=table_name, table_schema=self._db_name)
        items = self.execute_select(self.t_columns, cols=cols, where_value=where_value)
        return items

    def add_table_column(self, t_name, col_name, col_type, col_comment='', allow_null=False, default_value=None):
        add_sql = u"ALTER TABLE {t_name} ADD COLUMN  {c_name} {c_type} {c_dv} {c_null} COMMENT '{c_comment}';"
        c_null = "" if allow_null is True else "NOT NULL"
        c_dv = "" if default_value is None else "DEFAULT %s" % default_value
        sql = add_sql.format(t_name=t_name, c_name=col_name, c_type=col_type, c_null=c_null, c_comment=col_comment,
                             c_dv=c_dv)
        self.execute(sql, print_sql=True)
        return True

    def update_table(self, table_desc):
        del_msg = "Table {table_name} Column {col_name} Not Exist In Latest Table Structure, Delete It Please Exec:\n"
        del_msg += "ALTER TABLE {table_name} DROP COLUMN {col_name};\n"

        t_name = table_desc["table_name"]
        cols_items = self.get_table_info(t_name)
        for desc_item in table_desc["table_cols"]:
            is_exist = False
            for i in range(len(cols_items) - 1, -1, -1):
                cols_item = cols_items[i]
                if cols_item["column_name"].lower() == desc_item["col_name"].lower():
                    is_exist = True

                    cols_items.remove(cols_item)
            if is_exist is False:
                desc_item["allow_null"] = True
                self.add_table_column(t_name, **desc_item)
                print("%s not exist" % desc_item["col_name"])
        if len(cols_items) > 0:
            for item in cols_items:
                sys.stderr.write(del_msg.format(table_name=t_name, col_name=item["column_name"]))
        return True

    def create_table(self, table_desc):
        if type(table_desc) != dict:
            return False, "table_desc need dict"
        if "table_name" not in table_desc or "table_cols" not in table_desc:
            return False, "Bad table_desc, need table_name and table_cols"
        exists_drop = table_desc.get("exists_drop", False)
        table_cols = table_desc["table_cols"]
        primary_key = []
        uni_key = []
        mul_key = []
        fields = []
        for col in table_cols:
            field_sql = ""
            if "col_name" not in col or "col_type" not in col:
                return False, "Bad col, need col_name and col_type"
            col_name = col["col_name"]
            field_sql += "%s %s" % (col_name, col["col_type"])
            if "allow_null" in col and col["allow_null"] is True:
                pass
            else:
                field_sql += " NOT NULL"
            if "auto_increment" in col and col["auto_increment"] is True:
                field_sql += " auto_increment"
                primary_key.append(col_name)
            elif "pri_key" in col and col["pri_key"] is True:
                primary_key.append(col_name)
            if "uni_key" in col and col["uni_key"] is True:
                uni_key.append(col_name)
            if "mul_key" in col and col["mul_key"] is True:
                mul_key.append(col_name)
            if "default_value" in col and col["default_value"] is not None:
                "".startswith("int")
                if col["col_type"].startswith("int") or col["col_type"].startswith("tinyint") or col["col_type"].startswith("bit"):
                    field_sql += " default %s" % col["default_value"]
                else:
                    field_sql += " default '%s'" % col["default_value"]

            if "col_comment" in col and col["col_comment"] is not None:
                field_sql += " COMMENT '%s'" % col["col_comment"]
            fields.append(field_sql)
        table_name = table_desc["table_name"]
        if len(primary_key) > 0:
            fields.append(" PRIMARY KEY (%s)" % ",".join(primary_key))
        if len(uni_key) > 0:
            for key in uni_key:
                fields.append(" UNIQUE KEY ({0})".format(key))
        if len(mul_key) > 0:
            for key in mul_key:
                fields.append(" INDEX {0} ({0})".format(key))
        execute_message = ""
        if "table_suffix" in table_desc and isinstance(table_desc["table_suffix"], list):
            if "table_comment" in table_desc and table_desc["table_comment"] != "":
                comment = table_desc["table_comment"]
            else:
                comment = "%s"
            for t_item in table_desc["table_suffix"]:
                t_name_item = "%s_%s" % (table_name, t_item)
                if self.table_exist(t_name_item) is True:
                    if exists_drop is False:
                        execute_message += "%s Table exist\n" % t_name_item
                        continue
                    print("drop")
                    self.execute("DROP TABLE %s" % t_name_item)
                create_table_sql = "CREATE TABLE %s ( %s )" % (t_name_item, ",".join(fields))
                item_comment = comment % t_item
                create_table_sql += " COMMENT '%s'" % item_comment
                create_table_sql += " DEFAULT CHARSET=utf8;"
                self.execute(create_table_sql)
                execute_message += "CREATE TABLE %s Success \n" % t_name_item
        else:
            if self.table_exist(table_name) is True:
                if exists_drop is False:
                    self.update_table(table_desc)
                    return False, "Table exist."
                self.execute("DROP TABLE %s" % table_name)
            create_table_sql = "CREATE TABLE %s ( %s )" % (table_name, ",".join(fields))
            if "table_comment" in table_desc and table_desc["table_comment"] != "":
                create_table_sql += " COMMENT '%s'" % table_desc["table_comment"]
            create_table_sql += " DEFAULT CHARSET=utf8;"
            self.execute(create_table_sql)
            execute_message += "CREATE TABLE %s Success \n" % table_name
        return True, execute_message

    def create_mul_table(self, mul_table_desc):
        if type(mul_table_desc) != list:
            return False, "mul_table_desc need list"
        mul_len = len(mul_table_desc)
        if mul_len <= 0:
            return False, "mul_table_desc len is 0"
        fail_index = []
        for i in range(mul_len):
            table_desc = mul_table_desc[i]
            result, message = self.create_table(table_desc)
            if result is False:
                fail_index.append((i, message))
        return True, fail_index

    def create_from_json_file(self, json_file):
        if os.path.isfile(json_file) is False:
            print("json file not exist")
            return False, "json file not exist"
        read_json = open(json_file)
        json_content = read_json.read()
        read_json.close()
        try:
            json_desc = json.loads(json_content, "utf-8")
        except ValueError as ve:
            print(ve)
            return False, "File content not json"
        result, message = self.create_mul_table(json_desc)
        return result, message

    def create_from_dir(self, desc_dir):
        if os.path.isdir(desc_dir) is False:
            return False, "desc dir not exist"
        desc_files = os.listdir(desc_dir)
        create_info = []
        for item in desc_files:
            if not item.endswith(".json"):
                continue
            file_path = desc_dir+ "/" + item
            result, info = self.create_from_json_file(file_path)
            create_info.append({"file_path": file_path, "create_result": result, "create_info": info})
        return True, create_info
