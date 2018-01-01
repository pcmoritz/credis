import multiprocessing
import time

import numpy as np

import common
from common import AckClient, GetHeadFromMaster, MasterClient, startcredis

ack_client = AckClient()
master_client = MasterClient()


def SeqPut(n, ops_completed):
    """For i in range(n), sequentially put i->i into redis."""
    p = ack_client.pubsub(ignore_subscribe_messages=True)
    p.subscribe("answers")
    head_client = GetHeadFromMaster(master_client)

    def Put(i):
        sn = head_client.execute_command("MEMBER.PUT", str(i), str(i))
        for ack in p.listen():  # Blocks.
            assert int(ack["data"]) == sn
            break

    latencies = []
    for i in range(n):
        start = time.time()
        Put(i)  # i -> i
        latencies.append((time.time() - start) * 1e6)  # Microsecs.
        time.sleep(0.001)
        ops_completed.value += 1  # No lock needed.

    nums = np.asarray(latencies)
    print('latency (us): mean %.5f std %.5f num %d' %
          (np.mean(nums), np.std(nums), len(nums)))


def test_demo():
    # Launch driver thread.
    n = 500
    ops_completed = multiprocessing.Value('i', 0)
    driver = multiprocessing.Process(target=SeqPut, args=(n, ops_completed))
    driver.start()

    # Kill head.
    time.sleep(0.5)
    common.KillNode(index=2)

    driver.join()
    print('Total ops %d, completed ops %d' % (n, ops_completed.value))
    assert ops_completed.value == n
