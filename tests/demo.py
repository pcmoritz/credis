import multiprocessing
import time

import numpy as np

import common
import redis
from common import *

ack_client = AckClient()
master_client = MasterClient()
head_client = GetHeadFromMaster(master_client)
act_pubsub = None

# Put() ops can be ignored when failures occur on the servers.  Try a few times.
fails_since_last_success = 0
max_fails = 3


# From redis-py/.../test_pubsub.py.
def wait_for_message(pubsub, timeout=0.1, ignore_subscribe_messages=False):
    now = time.time()
    timeout = now + timeout
    while now < timeout:
        message = pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages)
        if message is not None:
            return message
        time.sleep(1e-5)  # 10us.
        now = time.time()
    return None


def Put(i):
    global head_client
    global ack_pubsub
    global fails_since_last_success

    put_issued = False
    for k in range(3):  # Try 3 times.
        try:
            sn = head_client.execute_command("MEMBER.PUT", str(i), str(i))
            put_issued = True
            break
        except redis.exceptions.ConnectionError:
            head_client = RefreshHeadFromMaster(master_client)  # Blocking.
            continue
    if not put_issued:
        raise Exception("Irrecoverable redis connection issue; put client %s" %
                        head_client)

    # Wait for the ack.
    ack = None
    good_client = False
    for k in range(3):  # Try 3 times.
        try:
            # if k > 0:
            print('k %d pubsub %s' % (k, ack_pubsub.connection))
            # NOTE(zongheng): 1e-4 seems insufficient for an ACK to be
            # delivered back.  1e-3 has the issue of triggering a retry, but
            # then receives an ACK for the old sn (an issue clients will need
            # to address).  Using 10ms for now.
            ack = wait_for_message(ack_pubsub, timeout=1e-2)
            good_client = True
            break
        except redis.exceptions.ConnectionError as e:
            _, ack_pubsub = RefreshTailFromMaster(master_client)  # Blocking.
            continue

    if not good_client:
        raise Exception("Irrecoverable redis connection issue; ack client %s" %
                        ack_pubsub.connection)
    elif ack is None:
        # Connected but an ACK was not received after timeout (the update was
        # likely ignored by the store).  Retry.
        fails_since_last_success += 1
        if fails_since_last_success >= max_fails:
            raise Exception(
                "A maximum of %d update attempts have failed; "
                "no acks from the store are received. i = %d, client = %s" %
                (max_fails, i, ack_pubsub.connection))
        _, ack_pubsub = RefreshTailFromMaster(master_client)
        print("%d updates have been ignored since last success, "
              "retrying Put(%d) with fresh ack client %s" %
              (fails_since_last_success, i, ack_pubsub.connection))
        time.sleep(0.01)
        Put(i)
    else:
        # TODO(zongheng): this is a stringent check.  See NOTE above: sometimes
        # we can receive an old ACK.
        assert int(ack["data"]) == sn
        fails_since_last_success = 0


def SeqPut(n, ops_completed):
    """For i in range(n), sequentially put i->i into redis."""
    global ack_pubsub
    _, ack_pubsub = AckClientAndPubsub(ack_client)

    latencies = []
    for i in range(n):
        print('i %d' % i)
        start = time.time()
        Put(i)  # i -> i
        latencies.append((time.time() - start) * 1e6)  # Microsecs.
        time.sleep(0.01)  # Allows time for nodes to be killed.
        ops_completed.value += 1  # No lock needed.

    nums = np.asarray(latencies)
    print('latency (us): mean %.5f std %.5f num %d' %
          (np.mean(nums), np.std(nums), len(nums)))


# Asserts that the redis state is exactly {i -> i | i in [0, n)}.
def CheckSeqRead(n):
    read_client = RefreshHeadFromMaster(master_client)
    assert len(read_client.keys(b'*')) == n
    for i in range(n):
        data = read_client.get(str(i))
        assert int(data) == i


def test_demo():
    # Launch driver thread.
    n = 1000
    ops_completed = multiprocessing.Value('i', 0)
    driver = multiprocessing.Process(target=SeqPut, args=(n, ops_completed))
    driver.start()

    # TODO(zongheng): AddNode() should always append new port to PORTS?
    # Kill / add.
    new_nodes = []
    time.sleep(0.1)
    # time.sleep(0.1)
    common.KillNode(index=1)
    new_nodes.append(common.AddNode(master_client))
    new_nodes.append(common.AddNode(master_client))
    # new_nodes.append(common.AddNode(master_client))
    # new_nodes.append(common.AddNode(master_client))
    # print("Added new server with port %d" % new_nodes[-1][1])

    driver.join()
    assert ops_completed.value == n
    CheckSeqRead(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()

    print('Total ops %d, completed ops %d' % (n, ops_completed.value))
