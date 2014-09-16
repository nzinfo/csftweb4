# -*- coding: utf-8 -*-
"""
    处理数据的导出
    [初始化数据分片]
    1 启动事务
    2 创建临时表，只有主表的主键
    3 记录相关表的 modify_time
    4 分批导出 主表的主键， 按page_size 分好，持久化
    5 放弃事务

    [导出数据]
    1 启动只读事务
    2 创建临时表
    3 关联主表与临时表
    4 关联从表与临时表
    5 保存数据，（标记为完成）
    6 放弃事务

"""


class DBSyncTaskBase(object):
    """
        数据库的同步，分为若干细小的任务，
        根据调度程序，分批次执行。
        执行上下文包括： 数据库连接， 数据持久化层
    """
    def __init__(self, tbl_mgr, rel_mgr, conn):
        self._table_mgr = tbl_mgr
        self._rel_mgr = rel_mgr
        self._conn = conn


class DBSyncTaskInitDataBatch(DBSyncTaskBase):
    """
        负责 [初始化数据分片]
    """
    def process(self, storage):
        """
            storage: 负责存储的接口
        """
        print storage


class DBSyncTaskSyncDataPackage(DBSyncTaskBase):
    """
        针对一个 具体的数据分区， 填充数据
    """


# end of file