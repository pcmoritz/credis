#!/bin/bash
set -ex

function setup() {
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
    sleep 1
    redis-cli -p 6369 MASTER.ADD 127.0.0.1 6379
    redis-cli -p 6369 MASTER.ADD 127.0.0.1 6380
    redis-cli -p 6369 MASTER.ADD 127.0.0.1 6381

    # Have chain nodes connect to master.
    redis-cli -p 6379 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
    redis-cli -p 6380 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
    redis-cli -p 6381 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
}

setup
