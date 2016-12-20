"""
Storage Manager

@author: FATESAIKOU
"""

import mmap
import json
import struct

import Utils

class StorageManager:
    def __init__(self, rdb_hashtable, rdb_content_offset, rdb_content, rdb_config, rdb_counter, mode):
        # initialize file path
        self.hashtable_path = rdb_hashtable
        self.content_offset_path = rdb_content_offset
        self.content_path = rdb_content
        self.config_path = rdb_config
        self.counter_path = rdb_counter
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

        # load database counter
        counter_src = open(self.counter_path, 'r')
        counter = json.loads(counter_src.read())
        counter_src.close()

        # mmap files
        self.m_hashtable = mmap.mmap(self.hashtable_src.fileno(), length=0, flags=mmap.MAP_SHARED)
        self.m_content_offset = mmap.mmap(self.content_offset_src.fileno(), length=0, flags=mmap.MAP_SHARED)
        self.m_content = mmap.mmap(self.content_src.fileno(), length=0, flags=mmap.MAP_SHARED)

        # initialize env vars
        self.cols = config['cols']
        self.key_col = config['key_col']
        self.key_col_index = [col['name'] for col in config['cols']].index(config['key_col'])
        self.hash_limit = config['hash_limit']
        self.hashtable_width = config["hashtable_width"]
        self.record_offset_width = config["record_offset_width"]

        # database counter
        self.hash_end = counter["hash_end"]
        self.record_num = counter["record_num"]
        self.content_end = counter["content_end"]

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
        search_result = self.__findRecord( record[self.key_col] )

        if search_result["status"] == "not-found":
            # Create content in rdb_content, and get content_offset
            content_creation_result = self.__createContent(record)

            # Create record_mete in rdb_content_offset, and get rid
            record_creation_result = self.__createRecord(content_creation_result["record-content-offset"])

            # Create record_hash in rdb_hashtable at entry result["new-hashtable-entry"]
            hash_creation_result = self.__createHash(record_creation_result["record-id"], search_result["new-hashtable-entry"])

            # Return hashtable_ele, rid, record, and status
            return {
                "status": "INFO: success",
                "hash-entry": hash_creation_result["aim-entry"],
                "record-id": record_creation_result["record-id"],
                "content-offset": content_creation_result["record-content-offset"],
                "content-data": content_creation_result["record-content"]
            }
            
        elif search_result["status"] == "found-parent":
            # Create content in rdb_content, and get content_offset
            content_creation_result = self.__createContent(record)

            # Create record_mete in rdb_content_offset, and get rid
            record_creation_result = self.__createRecord(content_creation_result["record-content-offset"])

            # Create record_hash in rdb_hashtable at entry result["new-hashtable-entry"], and Linking it to the old record.
            hash_creation_result = self.__linkHash(record_creation_result["record-id"], search_result["parent-hashtable-entry"])

            # Return hashtable_ele, rid, record, and status
            return {
                "status": "INFO: do linking",
                "hash-entry": hash_creation_result["aim-entry"],
                "record-id": record_creation_result["record-id"],
                "content-offset": content_creation_result["record-content-offset"],
                "content-data": content_creation_result["record-content"]
            }
        else:
            return { "status": "FAIL: key-exist" }
        
    def getByKey(self, key_val):
        search_result = self.__findRecord(key_val)
        
        if search_result["status"] == "found":
            return {
                "status": "INFO: record-found",
                "record": search_result["record-data"]
            }
        else:
            return {
                "status": "INFO: record-not-found",
            }

    def getByRid(self, rid):
        if self.record_num - 1 < rid:
            return { "status": "INFO: record-not-found" }
        
        record = self.__getRecord(rid)

        if record["is-deleted"] == False:
            return {
                "status": "INFO: record-found",
                "record": record
            }
        else:
            return { "status": "INFO: record-not-found2" }

    def delete():
        print "delete"

    def __findRecord(self, key_val):
        # get hash value
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
                    "hashtable-content": (rid, next),
                    "record-data": record_data
                }
            # If key_val is not equal, but it have the next, GO FOR IT.
            elif next != -1:
                now_entry = next
            # If it have no next, but have record, than IT COULD BE THE PARENT!
            else:
                return {
                    "status": "found-parent",
                    "parent-hashtable-entry": now_entry,
                    "parent-hashtable-content": (rid, next)
                }

    def __getRecord(self, rid):
        content_offset = struct.unpack("i", self.m_content_offset[rid * 4:rid * 4 + 4])[0]
        
        if content_offset < 0:
            content_offset = (content_offset + 1) * -1
            is_deleted = True
        else:
            is_deleted = False
        
        record_data = { "is-deleted": is_deleted }
        cur_offset = content_offset
        for col in self.cols:
            # Get data size
            data_size = struct.unpack("i", self.m_content[cur_offset:cur_offset + 4])[0]
            cur_offset = cur_offset + 4

            # Get data
            if col["type"] == 'int':
                record_data[ col["name"] ] = struct.unpack("i", self.m_content[cur_offset:cur_offset + data_size])[0]
            elif col["type"] == 'string':
                record_data[ col["name"] ] = struct.unpack("%ds" % data_size, self.m_content[cur_offset:cur_offset + data_size])[0]

            cur_offset = cur_offset + data_size

        return record_data

    def __linkHash(self, rid, parent_entry):
        # If size too small mmaping it, expand
        if self.m_hashtable.size() < (self.hash_end * 8) + 8:
            self.m_hashtable.resize( self.m_hashtable.size() + 1024 )

        # Create Hash Record
        hash_creation_result = self.__createHash(rid, self.hash_end)

        # Update hashcounter
        self.hash_end = self.hash_end + 8

        return hash_creation_result

    def __createHash(self, rid, aim_entry):
        # Check size at linkHash!!

        # Some needed variables
        hash_offset = aim_entry * self.hashtable_width
        
        # Encode & Write
        self.m_hashtable[hash_offset:hash_offset + self.hashtable_width] = struct.pack("ii", rid, -1)

        return {
            "record-id": rid,
            "aim-entry": aim_entry
        }

        print "To Insert the (rid, -1) to the aim entry in hashtable."

    def __createRecord(self, content_offset):
        # Some needed variables
        rid = self.record_num
        self.record_num = self.record_num + 1
        record_offset = rid * self.record_offset_width

        # If size too small mmapping it, expand.
        if self.m_content_offset.size() < record_offset + 4:
            self.m_content_offset.resize( self.m_content_offset.size() + 1024 )

        # Encode data & Write
        self.m_content_offset[record_offset:record_offset + self.record_offset_width] = struct.pack("i", content_offset)

        return {
            "record-id": rid,
            "content-offset": content_offset
        }


    def __createContent(self, record):
        # Encode data
        write_content = ''.join([Utils.packData(record[ col["name"] ]) for col in self.cols])
        
        # If size too small mmapping it, expand.
        if self.m_content.size() < self.content_end + len(write_content):
            self.m_content.resize( self.m_content.size() + max(len(write_content), 10240) )

        # Write data into it 
        record_offset = self.content_end
        self.m_content[self.content_end:self.content_end + len(write_content)] = write_content
        self.content_end = self.content_end + len(write_content)
        
        return {
            "record-content-offset": record_offset,
            "record-content": record
        }

    def __commitData(self):
        self.m_content.flush()
        self.m_content_offset.flush()
        self.m_hashtable.flush()
