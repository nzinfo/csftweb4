# -*- coding: utf-8 -*-
"""
    封装 YAML 文件读取的 表关系
"""


class DBTableRelation(object):

    def __init__(self, app_config, table_rel_config, tbl_mgr):
        self._app_config = app_config
        self._table_rel = table_rel_config
        self._main_table_root = None
        self._main_tables = []
        self._tbl_mgr = tbl_mgr

    def get_main_tables(self):
        """
            返回， 主表 + 与主表 1 vs. 1 关联的表
            1 表定义必须存在
        """
        if self._main_table_root:
            return self._main_table_root, self._main_tables

        main_tables = self._table_rel['main']
        main_table = None
        sub_main_tables = list()
        for t in main_tables:
            if 'pk' in main_tables[t]:
                if main_table:
                    raise KeyError("multi pk section found in %s " % t)
                main_table = t
            else:
                if 'join' not in main_tables[t]:
                    raise KeyError("missing join section. %s " % t)
                sub_main_tables.append(t)
        self._main_table_root = main_table
        self._main_tables = sub_main_tables
        return main_table, sub_main_tables

    def get_main_table_primary(self):
        main_tables = self._table_rel['main']
        main_tbl_def = main_tables[self._main_table_root]
        return main_tbl_def['pk']

    def get_table_join_on(self, tbl_name):
        if tbl_name == self._main_table_root:
            return None

        main_tables = self._table_rel['main']
        joint_tables = self._table_rel['join']
        if tbl_name in main_tables:
            return main_tables[tbl_name]['join']
        if tbl_name in joint_tables:
            return joint_tables[tbl_name]['join']
        return None

    def get_table_index(self, tbl_name):
        tables = self._table_rel['dict']
        reverse_fields = ['field']
        indexes = {}
        for k in tables[tbl_name]:
            if k not in reverse_fields and \
                    ('type' not in tables[tbl_name][k] or tables[tbl_name][k]['type'] == 'index'):
                indexes[k] = tables[tbl_name][k]
        return indexes

    def is_main_table(self, tbl_name):
        if not self._main_table_root:
            self.get_main_tables()
        if tbl_name == self._main_table_root:
            return True
        if tbl_name in self._main_tables:
            return True
        return False

    def get_join_tables(self):
        """
            取得与主表相关的关联表
            1 关联表 与 主表不能重名
            2 关联表 必须 存在于表定义中
            可以整个配置， 都没有 关联表
        """
        if 'join' not in self._table_rel:
            return []
        return self._table_rel['join'].keys()

    def get_dict_tables(self):
        if 'dict' not in self._table_rel:
            return []
        return self._table_rel['dict'].keys()


# end of file