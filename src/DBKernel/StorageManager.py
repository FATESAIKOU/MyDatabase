"""
Storage Manager

@author: FATESAIKOU
"""

import mmap
import json

class StorageManager:
    def __init__(self, rdb_hashtable, rdb_content_offset, rdb_content, rdb_config, mode):
        # initialize file path
        self.hashtable_path = rdb_hashtable
        self.content_offset_path = rdb_content_offset
        self.content_path = rdb_content
        self.config_path = rdb_config
        self.mode = mode

    def __enter__(self):
        # open the regular files
        self.hashtable_src = open(self.hashtable_path, 'ab+')
        self.content_offset_src = open(self.content_offset_path, 'ab+')
        self.content_src = open(self.content_path, 'ab+')

        # load database config
        config_src = open(self.config_path, 'r')
        self.config = json.loads(config_src.read())
        config_src.close()

        # mmap files
        self.m_hashtable = mmap.mmap(self.hashtable_src.fileno(), length=0, flags=mmap.MAP_SHARED)
        self.m_content_offset = mmap.mmap(self.content_offset_src.fileno(), length=0, flags=mmap.MAP_SHARED)
        self.m_content = mmap.mmap(self.content_src.fileno(), length=0, flags=mmap.MAP_SHARED)

        return self

    def __exit__(self, type, msg, traceback):
        # close mmaped files
        self.m_hashtable.close()
        self.m_content_offset.close()
        self.m_content.close()
        
        # close regular files src
        self.hashtable_src.close()
        self.content_offset_src.close()
        self.content_src.close()

        return False

    def __del__(self):
        self.__exit__()
    
    def test(self):
        return [
            self.m_hashtable.read(self.m_hashtable.size()),
            self.m_content_offset.read(self.m_content_offset.size()),
            self.m_content.read(self.m_content.size())
        ]

    def put():
        print "put"

    def get():
        print "get"

    def delete():
        print "delete"
