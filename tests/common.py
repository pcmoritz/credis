import subprocess
import time

import pytest

import redis

# Ports, in the order of [master; replicas in chain].

PORTS = [6369, 6370]
# SeqPut
# latency (us): mean 1716.46547 std 435.77646 num 2000
PORTS = [6369, 6370, 6371, 6372]
# SeqPut
# latency (us): mean 1620.17155 std 334.57320 num 2000
PORTS = [6369, 6370, 6371]


@pytest.fixture(scope="session", autouse=True)
def startcredis(request):
    assert len(PORTS) > 1, "At least 1 master and 1 chain node"
    master = subprocess.Popen([
        "redis/src/redis-server", "--loadmodule", "build/src/libmaster.so",
        "--port",
        str(PORTS[0])
    ])
    request.addfinalizer(master.kill)

    for port in PORTS[1:]:
        member = subprocess.Popen([
            "redis/src/redis-server", "--loadmodule", "build/src/libmember.so",
            "--port",
            str(port)
        ])
        request.addfinalizer(member.kill)
    time.sleep(2.0)

    master_client = redis.StrictRedis("127.0.0.1", PORTS[0])
    for port in PORTS[1:]:
        master_client.execute_command("MASTER.ADD", "127.0.0.1", str(port))


def AckClient():
    return redis.StrictRedis("127.0.0.1", PORTS[-1])


def AckClientAndPubsub(client=None):
    if client is None:
        client = AckClient()
    ack_pubsub = client.pubsub(ignore_subscribe_messages=True)
    ack_pubsub.subscribe("answers")
    return client, ack_pubsub


def MasterClient():
    return redis.StrictRedis("127.0.0.1", PORTS[0])


head_client = redis.StrictRedis("127.0.0.1", PORTS[1])


def GetHeadFromMaster(master_client):
    return head_client


def RefreshHeadFromMaster(master_client):
    head_addr_port = master_client.execute_command("MASTER.REFRESH_HEAD")
    print('head_addr_port: %s' % head_addr_port)
    splits = head_addr_port.split(b':')
    return redis.StrictRedis(splits[0], int(splits[1]))


def RefreshTailFromMaster(master_client):
    print('calling MASTER.REFRESH_TAIL')
    tail_addr_port = master_client.execute_command("MASTER.REFRESH_TAIL")
    print('tail_addr_port: %s' % tail_addr_port)
    splits = tail_addr_port.split(b':')
    c = redis.StrictRedis(splits[0], int(splits[1]))
    return AckClientAndPubsub(c)


def KillNode(index):
    assert index >= 0 and index < len(
        PORTS) - 1, "index %d num_chain_nodes %d" % (index, len(PORTS) - 1)
    port_to_kill = PORTS[index + 1]
    print('killing port %d' % port_to_kill)
    subprocess.check_output(["pkill", "redis-server.*:%s" % port_to_kill])
    del PORTS[index + 1]
