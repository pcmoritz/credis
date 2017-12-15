#!/bin/bash
set -ex

function AddTail() {
    ./redis/src/redis-server --loadmodule ./build/src/libmember.so --port 6382 &> newtail.log &
    sleep 1
    redis-cli -p 6369 MASTER.ADD 127.0.0.1 6382
    redis-cli -p 6382 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
}

#################### Head failure ####################

./setup.sh
redis-cli -p 6379 member.put k1 v1

# Kill head.
pkill 'redis-server.*:6379'
# Simulate liveness detection: explicitly tell master that a node has died.
redis-cli -p 6369 master.remove 127.0.0.1 6379
# Check we can still read.
redis-cli -p 6381 read k1
# Check we can write to the new head (originally middle).
redis-cli -p 6380 member.put k2 v2
# ...and read it back.
redis-cli -p 6381 read k2

# Recovery: add a new node (tail).
AddTail

# The new tail should start serving reads.
redis-cli -p 6382 read k1
redis-cli -p 6382 read k2
# New writes should propagate.
redis-cli -p 6380 member.put k3 v3
redis-cli -p 6382 read k3
# Check all SNs are the same.
redis-cli -p 6380 member.sn
redis-cli -p 6381 member.sn
redis-cli -p 6382 member.sn

#################### Tail failure ####################

pkill -f redis-server; sleep 0.5
./setup.sh
redis-cli -p 6379 member.put k1 v1

# Kill tail.
pkill 'redis-server.*:6381'
# Simulate liveness detection: explicitly tell master that a node has died.
redis-cli -p 6369 master.remove 127.0.0.1 6381
# Check we can still write.
redis-cli -p 6379 member.put k2 v2
# Check we can read from the new tail (originally middle).
redis-cli -p 6380 read k1
redis-cli -p 6380 read k2

# Recovery: add a new node (tail).
AddTail

# The new tail should start serving reads.
redis-cli -p 6382 read k1
redis-cli -p 6382 read k2
# New writes should propagate.
redis-cli -p 6379 member.put k3 v3
redis-cli -p 6382 read k3
# Check all SNs are the same.
redis-cli -p 6379 member.sn
redis-cli -p 6380 member.sn
redis-cli -p 6382 member.sn

#################### Middle failure ####################

pkill -f redis-server; sleep 0.5
./setup.sh
redis-cli -p 6379 member.put k1 v1

# Kill tail.
pkill 'redis-server.*:6380'
# Simulate liveness detection: explicitly tell master that a node has died.
redis-cli -p 6369 master.remove 127.0.0.1 6380
# Check we can still read.
redis-cli -p 6381 read k1
# Check we can still write.
redis-cli -p 6379 member.put k2 v2
redis-cli -p 6381 read k2

# Recovery: add a new node (tail).
AddTail

# The new tail should start serving reads.
redis-cli -p 6382 read k1
redis-cli -p 6382 read k2
# New writes should propagate.
redis-cli -p 6379 member.put k3 v3
redis-cli -p 6382 read k3
# Check all SNs are the same.
redis-cli -p 6379 member.sn
redis-cli -p 6381 member.sn
redis-cli -p 6382 member.sn
