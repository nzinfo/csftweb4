# -*- coding: utf-8 -*-
"""
    用于处理 ObjSpace 的命令行接口
"""

import json
import os
import codecs
import flask
from flask import Flask
from flask.ext.script import Manager, Command, Option
import csftweb


def create_app(config=None):
    # configure your app
    app = Flask(__name__)
    if config is None:
        #print app.root_path
        config = os.path.join(app.root_path, 'production.conf.py')

    app.config.from_pyfile(config)
    app.config.DEBUG = app.config['DEBUG']
    return app


def script_path(file_macro):
    return os.path.abspath(os.path.dirname(file_macro))


def get_connection_by_app(app, app_config):
    # check DB conn
    # DatabaseURL or Database(dict{})o
    if 'DatabaseURL' in app_config:
        engine = csftweb.cs_create_engine(app, app_config['DatabaseURL'])
    if 'Database' in app_config:
        raise NotImplementedError("add dict based database setting.")
    conn = engine.connect()
    if 'DatabaseSchema' in app_config:
        conn = csftweb.set_default_schema(conn, app_config['DatabaseSchema'])
    return conn


class DatabaseSchema(Command):
    """
        读取对应的数据库配置，
        - 此处的数据库名称，为配置文件中的名称
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='action', dest='action', help='import|genpy'),
        Option("-f", "--force", dest='force_flag', metavar='force overwrite', required=False, default=False,
               help='switch force update'),
        Option(metavar='appname', dest='app_name', help='app name'),
        Option(metavar='tblname', dest='tbl_names', help='table name', nargs='*'),
    )

    def run(self, debug_flag, action, force_flag, app_name, tbl_names):
        """
            1 从配置文件中读取 App 的配置信息
            2 连接到数据库
            3 读取全部的表信息
        """
        app = flask.current_app
        app_config = app.config['APP']
        app_name = app_name.lower()
        if app_name not in app_config:
            print ("No such app %s." % app_name)
            return
        app_config = app_config[app_name]

        meta_path = os.path.join(os.path.abspath(app_config['BasePath']), 'meta')
        if 'Path' in app_config and 'meta' in app_config['Path']:
            meta_path = os.path.abspath(app_config['Path']['meta'])

        if action == "import":
            DatabaseSchema.action_generate_table_define(app, app_config, meta_path,
                                                        debug_flag, force_flag, app_name, tbl_names)
        if action == 'genpy':
            DatabaseSchema.action_generate_sqlalchemy_define(app, app_config, meta_path,
                                                             debug_flag, force_flag, app_name, tbl_names)
        pass

    @staticmethod
    def action_generate_table_define(app, app_config, meta_path, debug_flag, force_flag, app_name, tbl_names):
        # check Schema
        with get_connection_by_app(app, app_config) as conn:
            insp = csftweb.DBInspector(conn)
            # fixme: 1 check targe file's existance; 2 generate required tables only.
            for tbl in insp.tables():
                tbl_def_filename = os.path.join(meta_path, tbl+".json")
                tbl_def_path, _ = os.path.split(tbl_def_filename)
                tbl_def = insp.table_define(tbl)
                if not os.path.isdir(tbl_def_path):
                    os.makedirs(tbl_def_path)
                with open(tbl_def_filename, 'w') as fh:
                    json.dump(tbl_def.to_jsonable(), fh, indent=4, sort_keys=True)
        return

    @staticmethod
    def action_generate_sqlalchemy_define(app, app_config, meta_path, debug_flag, force_flag, app_name, tbl_names):
        # 将用于生成 python code 所在的位置
        app_name = app_config['AppName']
        # touch files
        sqlalchemy_schema_path = os.path.join(os.path.abspath(app_config['BasePath']), app_name, 'schema')
        _init_pkg = os.path.join(os.path.abspath(app_config['BasePath']), app_name, '__init__.py')
        _init_schema = os.path.join(os.path.abspath(app_config['BasePath']), app_name, 'schema', '__init__.py')
        schema_file = "schema_%s.py" % app_name.lower()

        # 确定目录存在
        if not os.path.isdir(sqlalchemy_schema_path):
            os.makedirs(sqlalchemy_schema_path)

        open(_init_pkg, 'a').close()
        # 初始化 schema
        if not os.path.isfile(_init_schema):
            with open(_init_schema, 'w') as fh:
                fh.write("from schema_%s import *\n" % app_name.lower())

        schema_file = os.path.join(sqlalchemy_schema_path, schema_file)
        with get_connection_by_app(app, app_config) as conn:
            code_gen = csftweb.DBSchemaCodeGen(conn)
            ctx = code_gen.generate(meta_path, dialect='db2')
            with codecs.open(schema_file, 'w', encoding='utf-8') as fh:
                fh.write(ctx)
        return

class DatabaseConfig(Command):
    """
        命令行形式的全局配置
        - mode 工作模式： 作为 meta & 作为 worker
        - meta Meta机器的地址
        - hostname 本机的名称
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option("-w", "--write", dest='write_flag', metavar='write config', required=False, default=False,
               help='update config'),
        Option(metavar='cmd', dest='cmd', help='action, in [mode|meta|hostname] '),
        Option(metavar='value', dest='value', help='new value', nargs=1),
    )

    def run(self, debug_flag, write_flag, cmd, value):
        """
            TODO:
            1 read setting able config ( settings.py ) in current script 's path
            2 check cmd in ['mode', 'meta', 'hostname']
        """
        app = flask.current_app


class DatabaseChannel(Command):
    """
        索引频道的配置
        - schema [channel_name] @meta 索引的字段的配置 | 涉及哪些表 （JSON 形式） [读写]
        - join [channel_name] [port]  @worker 加入某个索引
        - leave [channel_name]  @worker 离开某个索引
        - worker [channel_name] 列出全部参与该 channel 的节点
        - list  列出全部的频道列表
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option("-w", "--write", dest='write_flag', metavar='write config', required=False, default=False,
               help='update config'),
        Option(metavar='cmd', dest='cmd', help='action, in [mode|meta|hostname] '),
        Option(metavar='value', dest='value', help='new value', nargs='*'),
    )


class DatabaseControl(Command):
    """
        控制集群
        - start [channel_name] @meta @worker 启动某个 channel ，如果是在 worker 上执行，则只影响到本 worker 的
        - stop  ...
        - start | stop with worker
        - status 集群的状态 | channel 的状态，最后一次重建时间等
        - if not channel_name, deal with the whole cluster `only @meta`

    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option("-w", "--worker", dest='worker', metavar='write node', required=False, default=None,
               help='especial worker'),
        Option(metavar='cmd', dest='cmd', help='action, in [start|stop|restart|status]'),
        Option(metavar='channel', dest='channel', help='channel name', nargs='?'),
    )


class DatabaseSync(Command):
    """
        同步数据
        - [database] [table] 同步
        - --all [database] [table] 同步全部数据
        如果不给出具体的表名，则为全部表同步
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option("--all", dest='flag_full_sync', metavar='full sync', required=False, default=False,
               help='do full sync'),
        Option(metavar='dbname', dest='db_name', help='database to be sync'),
        Option(metavar='tblname', dest='tbl_name', help='table name', nargs='?'),
    )


class DatabasePolicy(Command):
    """
        配置同步数据的策略
        - policy_log [database] [table] 创建同步需要的日志表需要的表结构与触发器
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='cmd', dest='cmd', help='action, in [policy_log]'),
        Option(metavar='dbname', dest='db_name', help='database to be sync'),
        Option(metavar='tblname', dest='tbl_name', help='table name', nargs='?'),
    )


class DatabaseIndex(Command):
    """
        创建索引
        - [channel_name]  @meta 全部重建
        - [channel_name]  @worker 只重建该worker上的
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='channel', dest='channel_name', help='index channel to be build.'),
        Option(metavar='tblname', dest='tbl_name', help='table name', nargs='?'),
    )

class DatabaseGenerate(Command):
    """
        Generate Python's ORM define.
        - PonyORM
        - SQLAlchemy
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='name', dest='db_names', help='generate python code for which database(s)', nargs='+'),
    )

    def run(self, debug_flag, db_names):
        app = flask.current_app
        for db in db_names:
            conn_str = app.config['DATABASE_%s_URL' % db.upper()]

            meta_path = app.config['%s_META_PATH' % db.upper()]
            meta_path = os.path.abspath(meta_path)

            app_path = app.config['%s_APP_PATH' % db.upper()]
            app_path = os.path.abspath(app_path)
            sqlalchemy_file = os.path.join(app_path, 'schema', 'sql_schema.py')
            app.db_engine = space.cs_create_engine(app, conn_str)
            gen = space.DBSchemaCodeGen(app.db_engine)
            if conn_str.find('mysql') == 0:
                code = gen.generate(meta_path, 'mysql', 'sqlalchemy')
                with open(sqlalchemy_file, 'wb') as fh:
                    fh.write(code)
                code = gen.generate(meta_path, 'mysql', 'pony')
            else:
                code = gen.generate(meta_path, 'postgresql', 'sqlalchemy')
            # write to file
            #print code
            pass


class DatabaseRebuild(Command):
    """
        Rebuild table & index
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='schema', dest='db_schema', help='which schema create table in.'),
        Option(metavar='name', dest='db_names', help='generate python code for which database(s)', nargs='+'),
    )

    def run(self, debug_flag, db_schema, db_names):
        app = flask.current_app
        for db in db_names:
            conn_str = app.config['DATABASE_%s_DEV_URL' % db.upper()]
            schema_cls_name = app.config['DATABASE_%s_SCHEMA_DEFINE' % db.upper()]
            #print schema_cls_name
            meta_path = app.config['%s_META_PATH' % db.upper()]
            meta_path = os.path.abspath(meta_path)
            app.db_engine = space.cs_create_engine(app, conn_str)
            # set schema.
            obj = space.load_class(schema_cls_name)
            if obj is None:
                print 'can not found %s.' % schema_cls_name
                return

            obj = obj()         # create the schema object.
            for tbl in obj._tables:
                #obj._tables[tbl].schema = db_schema
                obj._tables[tbl].drop(app.db_engine, checkfirst=True)
                obj._tables[tbl].create(app.db_engine, checkfirst=True)
            pass


class DatabaseSync(Command):
    """
        Sync Data from SQLDatabase -> ObjStore(LedisDB)
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='schema', dest='db_schema', help='which schema create table in.'),
        Option(metavar='name', dest='db_names', help='generate python code for which database(s)', nargs='+'),
    )

    def run(self, debug_flag, db_schema, db_names):
        app = flask.current_app
        for db in db_names:
            conn_str = app.config['DATABASE_%s_DEV_URL' % db.upper()]
            schema_cls_name = app.config['DATABASE_%s_SCHEMA_DEFINE' % db.upper()]
            #print schema_cls_name
            meta_path = app.config['%s_META_PATH' % db.upper()]
            meta_path = os.path.abspath(meta_path)
            app.db_engine = space.cs_create_engine(app, conn_str, True)
            # set schema.
            obj = space.load_class(schema_cls_name)
            if obj is None:
                print 'can not found %s.' % schema_cls_name
                return

            db_syncer = space.DBSync(app.db_engine)

            obj = obj()         # create the schema object.
            for tbl in obj._tables:
                db_syncer.sync_table(tbl, obj._tables[tbl])
                #print tbl


def setup_manager(app):
    mgr = Manager(app)
    mgr.add_command('schema', DatabaseSchema())
    #mgr.add_command("config", DatabaseConfig())
    #mgr.add_command("channel", DatabaseChannel())
    #mgr.add_command("ctrl", DatabaseControl())
    #mgr.add_command("sync", DatabaseSync())
    #mgr.add_command("index", DatabaseIndex())
    # TODO: runserver 有 两个模式  @meta @worker
    return mgr

if __name__ == "__main__":
    manager = setup_manager(create_app)
    manager.add_option('-c', '--config', dest='config', required=False)
    manager.run()


# end of file
