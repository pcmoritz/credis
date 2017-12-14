#!/bin/bash
set -ex

pushd build
make clean
make -j
popd

# Head, Middle, Tail.
./redis/src/redis-server --loadmodule ./build/src/libmember.so --port 6379 &> head.log &
./redis/src/redis-server --loadmodule ./build/src/libmember.so --port 6380 &> middle.log &
./redis/src/redis-server --loadmodule ./build/src/libmember.so --port 6381 &> tail.log &

# Master.
./redis/src/redis-server --loadmodule ./build/src/libmaster.so --port 6369 &> master.log &
sleep 3
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6379
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6380
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6381

# Have chain nodes connect to master.
redis-cli -p 6379 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
redis-cli -p 6380 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
redis-cli -p 6381 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369

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
