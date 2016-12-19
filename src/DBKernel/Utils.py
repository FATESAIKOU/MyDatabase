"""
Utils

@author: FATESAIKOU
"""

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
