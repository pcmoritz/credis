#!/bin/bash
set -ex

# Defaults to 1, or 1st arg if provided.
# Master is assigned port 6369.  Chain node i gets port 6369+i.
NUM_NODES=${1:-1}

function setup() {
    pushd build
    make -j
    popd

    # Master.
    ./redis/src/redis-server --loadmodule ./build/src/libmaster.so --port 6369 &> master.log &

    port=6369
    for i in $(seq 1 $NUM_NODES); do
      port=$(expr $port + 1)
      ./redis/src/redis-server --loadmodule ./build/src/libmember.so --port $port &> $port.log &
      sleep 0.1
      redis-cli -p 6369 MASTER.ADD 127.0.0.1 $port

      # Have chain nodes connect to master.
      sleep 0.1
      redis-cli -p $port MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
    done
}

setup
