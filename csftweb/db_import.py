# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
    实际处理 表结构的导入
    1 遍历所有表
    2 检查表的字段定义、索引的定义、主键的定义、外键的定义。

"""
import json
from .db_conn import cs_create_engine
import sqlalchemy
from sqlalchemy import inspect


class DBTableMeta(object):
    def __init__(self, tbl_name):
        self._tbl_name = tbl_name
        self._column = []
        self._primary = []
        self._foreign = []
        self._index = []
        self._unique = []

    def type_convert(self, c, dialect=False):
        """
            将用类表示的数据字段类型，转为 字符串表示，
            目前不支持 方言。 如果支持，只支持 MySQL | PostgreSQL
        """
        """
        sqlalchemy.types.BigInteger
        sqlalchemy.types.Boolean
        sqlalchemy.types.Date
        sqlalchemy.types.DateTime
        sqlalchemy.types.Enum
        sqlalchemy.types.Float
        sqlalchemy.types.Integer
        sqlalchemy.types.Interval
        sqlalchemy.types.LargeBinary
        sqlalchemy.types.Numeric
        sqlalchemy.types.PickleType
        sqlalchemy.types.SchemaType
        sqlalchemy.types.SmallInteger
        sqlalchemy.types.String
        sqlalchemy.types.Text
        sqlalchemy.types.Time
        sqlalchemy.types.Unicode
        sqlalchemy.types.UnicodeText
        """

        # WON'T FIX: 此处可以把代码写的很漂亮， 但是为了 后面 处理类型的灵活性， 处理为 IF
        # SQL Standard Types
        if issubclass(c, sqlalchemy.types.BIGINT):
            return "BIGINT"

        if issubclass(c, sqlalchemy.types.BINARY):
            return "BINARY"

        if issubclass(c, sqlalchemy.types.BLOB):
            return "BLOB"

        if issubclass(c, sqlalchemy.types.BOOLEAN):
            return "BOOLEAN"

        if issubclass(c, sqlalchemy.types.CHAR):
            return "CHAR"

        if issubclass(c, sqlalchemy.types.CLOB):
            return "CLOB"

        if issubclass(c, sqlalchemy.types.DATE):
            return "DATE"

        if issubclass(c, sqlalchemy.types.DATETIME):
            return "DATETIME"

        if issubclass(c, sqlalchemy.types.DECIMAL):
            return "DECIMAL"

        if issubclass(c, sqlalchemy.types.FLOAT):
            return "FLOAT"

        if issubclass(c, sqlalchemy.types.INTEGER):
            return "INTEGER"

        if issubclass(c, sqlalchemy.types.NCHAR):
            return "NCHAR"

        if issubclass(c, sqlalchemy.types.NVARCHAR):
            return "NVARCHAR"

        if issubclass(c, sqlalchemy.types.NUMERIC):
            return "NUMERIC"

        if issubclass(c, sqlalchemy.types.REAL):
            return "REAL"

        if issubclass(c, sqlalchemy.types.SMALLINT):
            return "SMALLINT"

        if issubclass(c, sqlalchemy.types.TEXT):
            return "TEXT"

        if issubclass(c, sqlalchemy.types.TIME):
            return "TIME"

        if issubclass(c, sqlalchemy.types.TIMESTAMP):
            return "TIMESTAMP"

        if issubclass(c, sqlalchemy.types.VARBINARY):
            return "VARBINARY"

        if issubclass(c, sqlalchemy.types.VARCHAR):
            return "VARCHAR"

        if dialect:
            if issubclass(c, sqlalchemy.dialects.mysql.TINYINT):
                return "TINYINT"

            if issubclass(c, sqlalchemy.dialects.mysql.TINYTEXT):
                return "TEXT"

        return None

    def to_jsonable(self):
        meta = {
            "name": self._tbl_name,
            "columns": [],
            "primary": self._primary,
            "foreign": self._foreign,
            "index": self._index,
            #"unique": self._unique         # 已经包括在  index 中
        }
        #with meta["columns"] as c:
        if True:
            for c in self._column:
                #print c.keys(), dir(c)
                column_meta = {
                    "name": c['name'],
                    "nullable": c['nullable'],
                    #"default": c['default'],
                    #"autoinc": c['autoincrement'],
                }

                if 'default' in c and c['default']:
                    column_meta['default'] = c['default']

                if 'autoincrement' in c:
                    column_meta['autoinc'] = c['autoincrement']

                c_expr = c['type'].column_expression
                c_type = self.type_convert(c_expr.im_class, True)
                column_meta['type'] = c_type
                if c_type in ['CHAR', 'NCHAR', 'NVARCHAR', 'VARBINARY', 'VARCHAR']:
                    column_meta['length'] = c['type'].length
                meta['columns'].append(column_meta)
                #FIXME: 暂时不处理限制 INTEGER 字节数的情况。
                #print dir(c['type'])
                #print dir(c_expr), c_expr
                #print issubclass(c_expr.im_class, sqlalchemy.types.CHAR)
                #print c_expr.im_class, c_expr.im_func, c_expr.im_self
                #print c['type'].length
                #print c['type'].python_type, c['type']._sqla_type, c['type'].charset, c['type'].collation
                #print col

        # 处理主键
        # 处理索引
        # 处理唯一索引
        # 处理外键
        return meta


class DBInspector(object):
    """

    """
    def __init__(self, engine):
        self._engine = engine
        if hasattr(self._engine, '__db_current_schema__'):
            self._schema = self._engine.__db_current_schema__
        else:
            self._schema = None
        self._table_names = []

    def tables(self):
        if len(self._table_names):
            return self._table_names
        inspector = inspect(self._engine)
        self._table_names = inspector.get_table_names(schema=self._schema)
        return self._table_names

    def table_define(self, tbl_name):
        inspector = inspect(self._engine)
        if tbl_name not in self.tables():
            return []  # no such table
        columns = inspector.get_columns(tbl_name, schema=self._schema)
        comments = {}
        """
        # might pass connect , need a check.
        if self._engine.uri.find('mysql') == 0:
            #select COLUMN_NAME, COLUMN_COMMENT from information_schema.columns where table_name="test_tbl"
            try:
                rs = self._engine.execute("select COLUMN_NAME, COLUMN_COMMENT from information_schema.columns "
                                          "where table_name='%s'" % tbl_name)
                for row in rs:
                    #print row
                    if row[1]:
                        comments[row[0]] = row[1]
            finally:
                #conn.close()
                pass
        """
        # postfix stick comment on column
        for col in columns:
            if col['name'] in comments:
                col['comment'] = comments[col['name']]

        # do table define
        tbl_meta = DBTableMeta(tbl_name)
        tbl_meta._column = columns
        tbl_meta._primary = inspector.get_pk_constraint(tbl_name)
        tbl_meta._foreign = inspector.get_foreign_keys(tbl_name)
        tbl_meta._index = inspector.get_indexes(tbl_name)
        #tbl_meta._unique = inspector.get_unique_constraints(tbl_name)
        return tbl_meta

    # end class

# end of file

