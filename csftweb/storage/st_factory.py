# -*- coding: utf-8 -*-


def create_storage(app_config):
    # check type
    if 'Storage' not in app_config or app_config['Storage'] == 'fs':
        from .st_filesystem import StorageFileSystem
        return StorageFileSystem(app_config)
    return None

# end of file
