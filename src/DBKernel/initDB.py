#!/usr/bin/env python

import struct
import json

import Utils

test_record = {
    "col1": "test",
    "col2": 1,
    "col3": 2
}

test_hash_limit = 2 * 1024 * 1024
test_hashvalue = Utils.hash33(test_record["col1"], 0, test_hash_limit)

test_rid = 0

test_content_offset = 0

# Init hashtable
rdb_hashtable_src = open("rdb_hashtable", "w")
rdb_hashtable_src.write(struct.pack("ii", -1, -1) * test_hash_limit)
rdb_hashtable_src.seek(test_hashvalue * 8)
rdb_hashtable_src.write(struct.pack("ii", 0, -1))
rdb_hashtable_src.close()

# Init offset table
rdb_content_offset_src = open("rdb_content_offset", "w")
rdb_content_offset_src.seek(test_rid * 4)
rdb_content_offset_src.write(struct.pack("i", test_content_offset))
rdb_content_offset_src.close()

# Init content table
rdb_content_src = open("rdb_content", "w")
rdb_content_src.seek(test_content_offset)
rdb_content_src.write( ''.join([Utils.packData(test_record[col]) for col in ["col1", "col2", "col3"]]) )
rdb_content_src.close()

config_src = open("config.json", "w")
config_src.write(json.dumps({
    "cols": [
        {"name": "col1", "type": "string", "key": True},
        {"name": "col2", "type": "int", "key": False},
        {"name": "col3", "type": "int", "key": False}
    ],
    "key_col": "col1",
    "hash_limit": 1024 * 1024 * 2,
    "content_end": 24
}))
config_src.close()
