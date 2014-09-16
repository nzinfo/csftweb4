# -*- coding: utf-8 -*-
import os
from sqlalchemy import create_engine
from sqlalchemy import inspect


class SqlServerCreator:
    # keep code for SqlServer support.
    #import pyodbc
    def __init__(self, conf):
        """Initialization procedure to receive the database name"""
        #self.db_name = db_name
        self.conf = conf
        if 'sql_port' not in self.conf:
          self.conf['sql_port'] = '1433'

    def __call__(self):
        import pyodbc
        """Defines a custom creator to be passed to sqlalchemy.create_engine
             http://stackoverflow.com/questions/111234/what-is-a-callable-in-python#111255"""
        """
            在某些情况下，需要给出服务器的实例名称， 例如：
            192.168.107.131\SQLEXPRESS
        """
        #conf['sql_user']
        if os.name == 'posix':
            return SqlServerCreator.pyodbc.connect('DRIVER={TDS};'
                                                   'Server=%s;'
                                                   'Database=%s;'
                                                   'UID=%s;'
                                                   'PWD=%s;'
                                                   'TDS_Version=8.0;'
                                                   'Port=%s;' % (self.conf['sql_host'], self.conf['sql_db'],
                                                                 self.conf['sql_user'], self.conf['sql_pass'],
                                                                 self.conf['sql_port']) )

        elif os.name == 'nt':
            # use development environment
            sql_conn_str = '''DRIVER={SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;Port=%s;''' \
                % (self.conf['sql_host'], self.conf['sql_db'], self.conf['sql_user'], self.conf['sql_pass'],  self.conf['sql_port'])
            #print sql_conn_str
            #exit(0)
            conn = SqlServerCreator.pyodbc.connect(sql_conn_str)
            return conn

# end of SqlServerCreator


def cs_create_engine(app, conn_str, echo=False):
    """
        create engine by db_type,
            db_type in ['mssql', 'mysql']
    """
    creators = {
        #"mssql": SqlServerCreator,
        #"mysql": MySqlCreator
    }
    #if db_type not in creators:
    #    return None

    if hasattr(app, 'db_engine') and app.db_engine:
        return app.db_engine

    #engine = create_engine('%s://' % db_type, creator=creators[db_type](conf)  )
    engine = create_engine(conn_str, echo=echo, convert_unicode=True)
    engine.uri = conn_str
    app.db_engine = engine

    return engine


def set_default_schema(conn, schema_name, with_check=True):
    inspector = inspect(conn)
    schemas = inspector.get_schema_names()
    s_name = None
    for s in schemas:
        if s.strip().lower() == schema_name.strip().lower():
            s_name = s
    #print engine.url.get_dialect().__name__
    if s_name and conn.engine.url.get_dialect().__name__ == 'DB2Dialect_ibm_db':
        # do set default schema
        # db2 set current schema = DB2OTHER
        conn.execute("set current schema = %s" % s_name)
    conn.__db_current_schema__ = s_name.strip().lower()
    return conn