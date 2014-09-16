# DEBUG or NOT
DEBUG = 1

"""
        代码路径包括：
        /CaseInfo       有关此频道的全部生成文件
            /idx        存放实际的索引
            /data       数据
                /db     创建索引前、创建增量索引临时存放数据的位置
                /rt     实时索引的 binlog
                __mark__    记录当前索引的主索引 与 从索引的区分条件
                        * 按主键
                        * 按最后一次更新时间

            /shard      频道对应的分布式信息（包括高可用），记录数据在节点间的分布情况
            /meta       [可选] 有关的表定义
            /filters    处理数据源用到的自定义的过滤器 （输入 ctx, row）
            /CaseInfo   与数据源相关的 Python 代码
                /schema     索引中， 各字段的定义
                - python 包， SQLAlchemy 风格的表结构定义
            /conf       对应的配置文件
            /dict       对应的分词法词库文件， 或 通过 RPC 方式时， 访问分词法的方式
            /log        [可选] 查询日志
            /gate       对外提供服务的接口
                /searchd.pid 对外提供检索服务的 searchd 的 pid
                或， 访问 可以控制 对应 searchd 的方式
        其中 meta & log 可以通过设置改到别处
"""


# CaseInfo  PartyInfo   LegalDocument
APP = {
    'ajxx': {           # search channel 案件信息 ajxx ， 此处的名称必须是小写的
        'AppName': 'CaseInfo',
        # DatabaseURL or  Database with db_type db_host db_port db_name db_uid db_pwd
        'DatabaseURL': 'ibm_db_sa://Administrator:123456@192.168.2.132:50000/GAOY',
        'DatabaseSchema': 'COURT',
        # 自动生成 代码的存放路径
        'BasePath': './CaseInfo',

        'Path': {
            """
                存放
                    - 表结构的定义
                    - 表与表之间的关联关系， 不具体定义到字段，仅仅到表
                        * 用于分页时，可以有效对数据进行分包
                        * 可能会造成大量的重复数据
                    - 字典表
                        * 对于字典表，数据为全局共享
                        * 在字典表上定义多个索引，使用 hash 方式，可以快速被程序找到
            """

            # 额外的， 存放对应数据库的表结构定义， 可以多个项目共享，如果不存在或为空， 则在代码目录中
            #'meta': '',
            #'log': ''
        },

    },
    'dsr': {           # 当事人
        'AppName': 'PartyInfo',
        # DatabaseURL or  Database with db_type db_host db_port db_name db_uid db_pwd
        'DatabaseURL': 'ibm_db_sa://Administrator:123456@192.168.2.132:50000/GAOY',
        'DatabaseSchema': 'COURT',
        # 自动生成 代码的存放路径
        'BasePath': './PartyInfo',
        'Path': {
        },

    }
}
