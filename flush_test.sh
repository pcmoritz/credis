#!/bin/bash
set -ex

./setup.sh

redis-cli -p 6379 member.put k1 v1
# (nil)
redis-cli -p 6381 get k1
# "v1"
redis-cli -p 6381 tail.checkpoint
# (integer) 1
redis-cli -p 6381 tail.checkpoint
# (integer) 0
redis-cli -p 6381 list.checkpoint
# k1: v1
redis-cli -p 6379 member.put k2 v2
# (nil)
redis-cli -p 6381 tail.checkpoint
# (integer) 1
redis-cli -p 6381 list.checkpoint
# k1: v1; k2: v2

# Flush 1 next.
redis-cli -p 6379 head.flush
# k1 is gone.
redis-cli -p 6381 keys '*'
# Flush another.
redis-cli -p 6379 head.flush
# k2 is gone.
redis-cli -p 6381 keys '*'
# READ will read from checkpoint file.
redis-cli -p 6381 read k1
redis-cli -p 6381 read k2
# Subsequent flushes will show nothing to flush.
redis-cli -p 6379 head.flush
