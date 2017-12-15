#!/bin/bash
set -ex

./setup.sh

# Tests below
echo

# All -1.
redis-cli -p 6379 MEMBER.SN
redis-cli -p 6380 MEMBER.SN
redis-cli -p 6381 MEMBER.SN
echo

# Nothing in checkpoint
redis-cli -p 6381 list.checkpoint

# To test TAIL.CHECKPOINT
sleep 3
redis-cli -p 6379 member.put k1 v1
# (nil)
redis-cli -p 6381 get k1
# "v1"
redis-cli -p 6381 tail.checkpoint
# (integer) 1
redis-cli -p 6381 list.checkpoint
# k1: v1
redis-cli -p 6381 tail.checkpoint
# (integer) 0
redis-cli -p 6379 member.put k1 v2
# (nil)
redis-cli -p 6379 member.put k2 v2
# (nil)
redis-cli -p 6381 tail.checkpoint
# (integer) 2
redis-cli -p 6381 list.checkpoint
# k1: v2; k2: v2
echo

# To test MEMBER.SN
redis-cli -p 6379 MEMBER.SN
redis-cli -p 6380 MEMBER.SN
redis-cli -p 6381 MEMBER.SN
