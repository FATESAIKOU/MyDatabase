#!/usr/bin/env python

import struct
import json

rdb_hashtable_src = open("rdb_hashtable", "w")
rdb_hashtable_src.write(struct.pack("ii", -1, -1) * 1024 * 1024 * 2)
rdb_hashtable_src.close()

rdb_content_offset_src = open("rdb_content_offset", "w")
rdb_content_offset_src.write(" ")
rdb_content_offset_src.close()

rdb_content_src = open("rdb_content", "w")
rdb_content_src.write(" ")
rdb_content_src.close()

config_src = open("config.json", "w")
config_src.write(json.dumps({
    "cols": [
        {"name": "col1", "type": "string", "key": True},
        {"name": "col2", "type": "int", "key": False},
        {"name": "col3", "type": "int", "key": False}
    ],
    "key_col": "col1",
    "hash_limit": 1024 * 1024 * 2
}))
config_src.close()
