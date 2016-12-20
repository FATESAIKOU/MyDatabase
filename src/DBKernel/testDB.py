#!/usr/bin/env python

from StorageManager import StorageManager

import sys
import random
import time

#__init__(self, rdb_hashtable, rdb_content_offset, rdb_content, rdb_config, rdb_counter, mode):
rdb_hashtable = sys.argv[4]
rdb_content_offset = sys.argv[5]
rdb_content = sys.argv[6]
rdb_config = sys.argv[7]
rdb_counter = sys.argv[8]


alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789'

action = sys.argv[1]
key_len = sys.argv[2]
iterate_time = int(sys.argv[3])


total_cost = 0
with StorageManager(rdb_hashtable, rdb_content_offset, rdb_content, rdb_config, rdb_counter, "") as sm:
    # Initialize Records
    records = [ {"col1": ''.join([random.choice(alphabet) for _ in key_len]), "col2": i, "col3": i} for i in range(iterate_time)]

    # Fill in records
    start = time.time()
    for record in records:
        sm.put(record)
    end = time.time()

    
    # if read by id
    if action == "read-by-id":
        record_num = sm.getCounter()["record_num"]
        
        start = time.time()
        for _ in range(iterate_time):
            sm.getByRid(random.randint(0, record_num - 1))
        end = time.time()

    # if read by key
    elif action == "read-by-key":
        record_num = sm.getCounter()["record_num"]
        
        start = time.time()
        for _ in range(iterate_time):
            sm.getByKey(random.choice(records)["col1"])
        end = time.time()

    # if delete by id
    elif action == "delete-by-id":
        record_ids = [i for i in range(sm.getCounter()["record_num"])]
        random.shuffle(record_ids)

        start = time.time()
        for rid in record_ids:
            sm.deleteByRid(rid)
        end = time.time()

    # if delete by key
    elif action == "delete-by-id":
        record_keys = [records[i]["col1"] for rid in range(sm.getCounter()["record_num"])]

        start = time.time()
        for key in random.shuffle(record_keys):
            sm.deleteByKey(key)
        end = time.time()

    # if create, do nothing
    total_cost = end - start
    

print "Totol cost: %f" % total_cost
