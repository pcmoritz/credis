import multiprocessing
import time

import numpy as np

import redis
from tests import common
from tests.common import *

ack_client = AckClient()
master_client = MasterClient()
head_client = GetHeadFromMaster(master_client)
act_pubsub = None

# Put() ops can be ignored when failures occur on the servers.  Try a few times.
fails_since_last_success = 0
max_fails = 3
ops_completed = multiprocessing.Value('i', 0)


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
        time.sleep(1)
        Put(i)
    else:
        # TODO(zongheng): this is a stringent check.  See NOTE above: sometimes
        # we can receive an old ACK.
        assert int(ack["data"]) == sn
        fails_since_last_success = 0


def SeqPut(n, sleep_secs):
    """For i in range(n), sequentially put i->i into redis."""
    global ack_pubsub
    global ops_completed
    _, ack_pubsub = AckClientAndPubsub(ack_client)
    ops_completed.value = 0

    latencies = []
    for i in range(n):
        # if i % 50 == 0:
        print('i = %d' % i)
        start = time.time()
        Put(i)  # i -> i
        latencies.append((time.time() - start) * 1e6)  # Microsecs.
        time.sleep(sleep_secs)
        ops_completed.value += 1  # No lock needed.

    nums = np.asarray(latencies)
    print('latency (us): mean %.5f std %.5f num %d' %
          (np.mean(nums), np.std(nums), len(nums)))


# Asserts that the redis state is exactly {i -> i | i in [0, n)}.
def Check(n):
    read_client, _ = RefreshTailFromMaster(master_client)
    actual = len(read_client.keys(b'*'))
    assert actual == n, "Written %d Expected %d" % (actual, n)
    for i in range(n):
        data = read_client.get(str(i))
        assert int(data) == i, i


def test_demo():
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    # Kill / add.
    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1)
    new_nodes.append(common.AddNode(master_client))
    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    chain = [s.split(b':')[-1] for s in chain]
    assert chain == [b'6370', b'6372'], 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()
    print('Total ops %d, completed ops %d' % (n, ops_completed.value))


def test_kaa():
    """Kill, add, add."""
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1)
    new_nodes.append(common.AddNode(master_client))
    new_nodes.append(common.AddNode(master_client))

    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    assert len(chain) == 2 - 1 + len(new_nodes), 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()


def test_multi_kill_add():
    """Kill, add a few times."""
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    # Kill / add.
    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1)  # 6371 dead
    new_nodes.append(common.AddNode(master_client))  # 6372
    common.KillNode(index=1)  # 6372 dead
    new_nodes.append(common.AddNode(master_client))  # 6373
    common.KillNode(index=0)  # 6370 dead, now [6373]
    new_nodes.append(common.AddNode(master_client))  # 6374
    new_nodes.append(common.AddNode(master_client))  # 6375
    # Now [6373, 6374, 6375].
    common.KillNode(index=2)  # 6375 dead, now [6373, 6374]

    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    chain = [s.split(b':')[-1] for s in chain]
    assert chain == [b'6373', b'6374'], 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()


def test_dead_old_tail_when_adding():
    # We set "sleep_secs" to a higher value.  So "kill tail", "add node" will
    # be trigered without a refresh request from the driver.  Master will have
    # the following view of its members:
    # init: [ live, live ]
    # kill: [ live, dead ]
    #    - master not told node 1 is dead
    # Tests that when adding, the master detects & removes the dead node first.

    # Launch driver thread.
    n = 5
    sleep_secs = 1
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    time.sleep(0.1)
    common.KillNode(index=1)
    proc, _ = common.AddNode(master_client)
    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    assert len(chain) == 2 - 1 + 1, 'chain %s' % chain
    Check(ops_completed.value)

    proc.kill()
