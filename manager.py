# -*- coding: utf-8 -*-
"""
    用于处理 ObjSpace 的命令行接口
"""

import json
import os
import sys
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
        与表结构定义相关
    """
    """
        读取对应的数据库配置，
        - 此处的数据库名称，为配置文件中的名称
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='action', dest='action', help='import|verify|genpy'),
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
        if action == 'verify':
            DatabaseSchema.action_verify_table_relation(app, app_config, debug_flag)

    @staticmethod
    def action_generate_table_define(app, app_config, meta_path, debug_flag, force_flag, app_name, tbl_names):
        """
            从数据库中，通过反射读取表信息，生成 JSON 形式的元信息
        """
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
        """
            生成 SQLAlchemy 形式的 表格定义
        """
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

    @staticmethod
    def action_verify_table_relation(app, app_config, debug_flag):
        """
            读取表关系定义 ，从 __table__ , 列印出机器自己的理解。
            __table__ 的格式为 YAML
                定义了， 主表（主表间的关系）， 从表（从表与主表的关系）， 字典表（字典表需要使用的索引），以及各表使用的字段
                如果没有给出字段，则说明为 select * from table
        """
        import yaml
        base_path = os.path.abspath(app_config['BasePath'])
        meta_path = os.path.join(os.path.abspath(app_config['BasePath']), 'meta')
        if 'Path' in app_config and 'meta' in app_config['Path']:
            meta_path = os.path.abspath(app_config['Path']['meta'])

        yaml_table_rel_define_filename = os.path.join(base_path, '__tables__')
        table_rel = yaml.load(file(yaml_table_rel_define_filename, 'r'))

        table_mgr = csftweb.DBMetaTableManager(app_config, meta_path)
        rel_mgr = csftweb.DBTableRelation(app_config, table_rel, table_mgr)
        # do the output
        """
            - 列出全部主表
            - 列出主表直接的关联关系
            - 列出全部从表 & 关联关系
            - 列出全部涉及到的字段
        """
        main_tbl, sub_main_tbls = rel_mgr.get_main_tables()

        # FIXME: move the following code to else where... too long.
        """
            输出主表关系
        """
        if True:  # the base(root) main table
            fh = sys.stdout
            fh.write('main table: {name}\n'.format(name=main_tbl))
            fh.write('  primary key: [%s]\n\n' % ', '.join(rel_mgr.get_main_table_primary()))
            # FIXME: output the select fields.

        def output_main_tbl(tbl):
            if True:
                fh = sys.stdout
                fh.write('main table: {name}\n'.format(name=tbl))
                join_rel = rel_mgr.get_table_join_on(tbl)
                fh.write('  join on: [%s]\n' % tbl)
                for k in join_rel:
                    fh.write('\t{k} = {v}\n'.format(k=k, v=join_rel[k]))
                fh.write('\n')

        def output_join_tbl(tbl):
            fh = sys.stdout
            fh.write('join table: {name}\n'.format(name=tbl))
            join_rel = rel_mgr.get_table_join_on(tbl)
            fh.write('  join on: [%s]\n' % tbl)
            for k in join_rel:
                fh.write('\t{k} = {v}\n'.format(k=k, v=join_rel[k]))
            fh.write('\n')

        def output_dict_tbl(tbl):
            fh = sys.stdout
            fh.write('dict table: {name}\n'.format(name=tbl))
            idxs = rel_mgr.get_table_index(tbl)
            fh.write('  index:\n')
            for k in idxs:
                fh.write('\t{k} = {v}\n'.format(k=k, v=idxs[k]['column']))
            fh.write('\n')

        for tbl in sub_main_tbls:
            output_main_tbl(tbl)

        """
            输出关联表
        """
        sys.stdout.write('---------------------------------\n')
        join_tbls = rel_mgr.get_join_tables()
        for tbl in join_tbls:
            output_join_tbl(tbl)

        sys.stdout.write('---------------------------------\n')
        dict_tbls = rel_mgr.get_dict_tables()
        for tbl in dict_tbls:
            output_dict_tbl(tbl)
        #print table_rel


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
    """
    """
        sync index [--dry-run] app_name
        sync main [--dry-run] app_name
        sync delta [--dry-run] app_name
        sync merge [--dry-run] app_name   把增量中更新的数据， 合并到主数据上（根据主键）
        sync clean [--dry-run] app_name
        Note:
            需要记录的关系
            block_id -> range_begin, range_end, count?
            pk -> instance_id, block_id       instance_id ，具体在那个库（储存设备上）
            block_id -> [pks]
    """
    option_list = (
        Option("-d", "--debug", dest='debug_flag', action="count", required=False, default=0,
               help='debug flag'),
        Option(metavar='action', dest='action', help='index|main|delta|merge'),
        Option("--dry-run", dest='dryrun_flag', metavar='dry run?', required=False, default=False,
               help="dry run to see what's happening"),
        Option(metavar='appname', dest='app_name', help='app name'),
    )

    def run(self, debug_flag, action, dryrun_flag, app_name):
        app = flask.current_app
        app_config = app.config['APP']
        app_name = app_name.lower()
        if app_name not in app_config:
            print ("No such app %s." % app_name)
            return
        app_config = app_config[app_name]

        action_funcs = {
            'index': DatabaseSync.action_index,
        }

        if action in action_funcs:
            return action_funcs[action](app, app_config, debug_flag, dryrun_flag, app_name)

    @staticmethod
    def get_mgr(app_config):
        import yaml
        base_path = os.path.abspath(app_config['BasePath'])
        meta_path = os.path.join(os.path.abspath(app_config['BasePath']), 'meta')
        if 'Path' in app_config and 'meta' in app_config['Path']:
            meta_path = os.path.abspath(app_config['Path']['meta'])

        yaml_table_rel_define_filename = os.path.join(base_path, '__tables__')
        table_rel = yaml.load(file(yaml_table_rel_define_filename, 'r'))

        table_mgr = csftweb.DBMetaTableManager(app_config, meta_path)
        rel_mgr = csftweb.DBTableRelation(app_config, table_rel, table_mgr)
        return table_mgr, rel_mgr

    @staticmethod
    def action_index(app, app_config, debug_flag, dryrun_flag, app_name):
        """
        1 make storage wrap
        2 make connect to db
        3 make sync task
        """
        table_mgr, rel_mgr = DatabaseSync.get_mgr(app_config)
        storage = csftweb.storage.create_storage(app_config)

        with get_connection_by_app(app, app_config) as conn:
            task = csftweb.tasks.DBSyncTaskInitDataBatch(table_mgr, rel_mgr, conn)
            task.process(storage)
        pass

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



def setup_manager(app):
    mgr = Manager(app)
    mgr.add_command('schema', DatabaseSchema())
    #mgr.add_command("config", DatabaseConfig())
    #mgr.add_command("channel", DatabaseChannel())
    #mgr.add_command("ctrl", DatabaseControl())
    mgr.add_command("sync", DatabaseSync())
    #mgr.add_command("index", DatabaseIndex())
    # TODO: runserver 有 两个模式  @meta @worker
    return mgr

if __name__ == "__main__":
    manager = setup_manager(create_app)
    manager.add_option('-c', '--config', dest='config', required=False)
    manager.run()


# end of file
