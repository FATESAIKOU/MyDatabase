"""
Utils

@author: FATESAIKOU
"""

import struct

def hash33(key, start, limit):
    if isinstance(key, int):
        return (key + start) % limit
    elif isinstance(key, str):
        hashval = 0
        for c in key:
            hashval = (hashval << 5) + hashval + ord(c)

        return (hashval + start) % limit
    else:
        raise Exception('Not Supported Type')

def packData(data):
    if isinstance(data, int):
        return struct.pack("Ii", 4, data)
    elif isinstance(data, str):
        str_len = len(data)
        return struct.pack("I%ds" % str_len, str_len, data)
    else:
        raise Exception('Not Supported Type')
