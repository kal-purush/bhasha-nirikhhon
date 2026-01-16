#!/usr/bin/env python3


import os


def scans(basepath, maxdepth=None, file_type=None):
    """
     maxdepth 为要扫描的子目录深度, None表示全部扫描, 0 表示只扫描当前目录
     file_type 为要扫描的文件类型,有且只有三个值: None, 'f', 'd'
    """
    if not isinstance(maxdepth, int) and maxdepth is not None:
        raise ValueError('maxdepth must be int!!')
    if file_type not in ('d', 'f', None):
        raise ValueError("file type must be None or 'f' or 'd' !!")
    with os.scandir(basepath) as it:
        for entry in it:
            if entry.is_dir():
                if file_type != 'f':
                    yield entry.path
                if maxdepth is None:
                    yield from scans(entry.path, None, file_type)
                elif maxdepth > 0:
                    yield from scans(entry.path, maxdepth-1, file_type)
            elif file_type != 'd':
                yield entry.path

