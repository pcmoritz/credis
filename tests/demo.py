import multiprocessing
import time

import numpy as np

import common
import redis
from common import AckClient, GetHeadFromMaster, MasterClient, startcredis, RefreshHeadFromMaster

ack_client = AckClient()
master_client = MasterClient()
head_client = GetHeadFromMaster(master_client)


def SeqPut(n, ops_completed):
    """For i in range(n), sequentially put i->i into redis."""
    p = ack_client.pubsub(ignore_subscribe_messages=True)
    p.subscribe("answers")

    def Put(i):
        global head_client
        issued = False
        for k in range(3):  # Try 3 times.
            try:
                sn = head_client.execute_command("MEMBER.PUT", str(i), str(i))
                issued = True
                break
            except redis.exceptions.ConnectionError as e:
                head_client = RefreshHeadFromMaster(master_client)  # Blocking.
                continue
        if issued:
            for ack in p.listen():  # Blocks.
                assert int(ack["data"]) == sn
                break
        else:
            raise Exception(
                "Irrecoverable redis connection issue; put client %s" %
                head_client)

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

    # TODO(zongheng): when killing middle, program hangs somewhere.
    # TODO(zongheng): when killing tail, handle ack client refresh.
    # Kill.
    time.sleep(0.05)
    common.KillNode(index=0)

    driver.join()
    # TODO(zongheng): issue a correctness check by reading back all i->i.
    print('Total ops %d, completed ops %d' % (n, ops_completed.value))
    assert ops_completed.value == n
