# -*- coding: utf-8 -*-
"""
    处理 JSON 的表定义
    - 读取全部的表
    - 读取表定义
    - 检查表是否存在
    - 检查表字段是否存在
    - 读取字段类型
"""
import os
import json


class DBMetaTable(object):
    def __init__(self, json_tbl_conf):
        self._json_conf = json_tbl_conf


class DBMetaTableManager(object):
    def __init__(self, app_config, meta_table_path):
        self._meta_path = meta_table_path
        tables = [d for d in os.listdir(self._meta_path) if os.path.isfile(os.path.join(self._meta_path, d))]
        self._tables = dict()
        for tbl in tables:
            tbl_name, _ = os.path.splitext(tbl)
            with open(os.path.join(self._meta_path, tbl), 'r') as fh:
                json_tbl_conf = json.load(fh)
                self._tables[tbl_name] = DBMetaTable(json_tbl_conf)
        # all table define load

    def get_table(self, tbl_name):
        """
            返回 一个 逻辑表的 定义， 如果 None 则表不存在
        """
        if tbl_name not in self._tables:
            return None
        return self._tables[tbl_name]

# end of file