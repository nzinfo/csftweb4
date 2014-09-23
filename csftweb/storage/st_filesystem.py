# -*- coding: utf-8 -*-
import os


class StorageFileSystem(object):
    def __init__(self, app_config):
        """
        'Storage': 'fs',    # 支持的类型应该有 fs sqlite mysql ledis
        'StorageFS':        # 如果有，可以额外指定储存的中间状态的数据
            /data       数据
                /dict   字典表的数据
                /db     创建索引前、创建增量索引临时存放数据的位置
                /rt     实时索引的 binlog
                __mark__    记录当前索引的主索引 与 从索引的区分条件
        """
        base_path = os.path.abspath(app_config['BasePath'])
        self._data_path = os.path.join(base_path, 'data')
        if 'StorageFS' in app_config:
            self._data_path = os.path.abspath(app_config['StorageFS'])

        # 更新好了数据目录


    def save_dict_batch(self, dict_name, items):
        """
            批量保存 字典表中的数据
        """
        dict_path = os.path.join(self._data_path, 'dict', dict_name)
        if not os.path.isdir(dict_path):
            os.makedirs(dict_path)
        print dict_path
        pass



# end of file