"""
Storage Manager

@author: FATESAIKOU
"""

import mmap
import json
import struct

import Utils

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
        config = json.loads(config_src.read())
        config_src.close()

        # mmap files
        self.m_hashtable = mmap.mmap(self.hashtable_src.fileno(), length=0, flags=mmap.MAP_SHARED)
        self.m_content_offset = mmap.mmap(self.content_offset_src.fileno(), length=0, flags=mmap.MAP_SHARED)
        self.m_content = mmap.mmap(self.content_src.fileno(), length=0, flags=mmap.MAP_SHARED)

        # initialize env vars
        self.cols = config['cols']
        self.key_col = config['key_col']
        self.key_col_index = [col['name'] for col in config['cols']].index(config['key_col'])
        self.hash_limit = config['hash_limit']
        self.hashtable_width = 8

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
   
    def test(self):
        return [
            self.m_hashtable.read(8),
            self.m_content_offset.read(self.m_content_offset.size()),
            self.m_content.read(self.m_content.size())
        ]

    def put(self, record):
        result = self.__findRecord(record)

        if result["status"] == "not-found":
            print "There is a space!!"
        elif result["status"] == "found-parent":
            print "There is a parent in it!"
        else:
            print "Record Already Exist!"


    def get():
        print "get"

    def delete():
        print "delete"

    def __findRecord(self, record):
        # get hash value
        key_val = record[self.key_col]
        hash_val = Utils.hash33(key_val, 0, self.hash_limit)

        # start triversal
        now_entry = hash_val
        while True:
            hashdata_offset = now_entry * self.hashtable_width
            rid = struct.unpack("i", self.m_hashtable[hashdata_offset:hashdata_offset + 4])[0]
            next = struct.unpack("i", self.m_hashtable[hashdata_offset + 4:hashdata_offset + 8])[0]

            # If it have no rid was recorded, THERE IS NO RECORD!
            if rid < 0:
                return {
                    "status": "not-found",
                    "new-hashtable-entry": now_entry
                }

            record_data = self.__getRecord(rid)

            # If key_val is equal, THE RECORD WAS FOUND.
            if record_data[self.key_col] == key_val:
                return {
                    "status": "found",
                    "hashtable-entry": now_entry,
                    "record-data": record_data
                }
            # If key_val is not equal, but it have the next, GO FOR IT.
            elif next != -1:
                now_entry = next
            # If it have no next, but have record, than IT COULD BE THE PARENT!
            else:
                return {
                    "status": "found-parent",
                    "parent-hashtable-entry": now_entry
                }

    def __getRecord(self, rid):
        content_offset = self.m_content_offset[rid * 4:rid * 4 + 4]
        
        record_data = {}
        cur_offset = content_offset
        for col in self.cols:
            # Get data size
            data_size = struct.unpack("i", self.m_content[cur_offset:cur_offset + 4])[0]
            cur_offset = cur_offset + 4

            # Get data
            record_data[ col["name"] ] = struct.unpack(col["type"][0], self.m_content[cur_offset:cur_offset + data_size])[0]
            cur_offset = cur_offset + data_size

        return record_data
