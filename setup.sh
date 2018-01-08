#!/bin/bash
set -ex

# Defaults to 1, or 1st arg if provided.
NUM_NODES=${1:-1}

function setup() {
    pushd build
    make clean
    make -j
    popd

    # Master.
    ./redis/src/redis-server --loadmodule ./build/src/libmaster.so --port 6369 &> master.log &

    # Head, Middle, Tail.
    port=6369
    for i in $(seq 1 $NUM_NODES); do
      port=$(expr $port + 1)
      ./redis/src/redis-server --loadmodule ./build/src/libmember.so --port $port &> $port.log &
      sleep 0.1
      redis-cli -p 6369 MASTER.ADD 127.0.0.1 $port
    done

    # Have chain nodes connect to master.
    # redis-cli -p 6379 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
    # redis-cli -p 6380 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
    # redis-cli -p 6381 MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
}

setup
