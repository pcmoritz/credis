import pytest
import redis
import subprocess
import time

# This script should be run from within the credis/ directory

@pytest.fixture(scope="session", autouse=True)
def startcredis(request):
    master = subprocess.Popen(["redis/src/redis-server", "--loadmodule", "build/src/libmaster.so", "--port", "6369"])
    request.addfinalizer(master.kill)
    member1 = subprocess.Popen(["redis/src/redis-server", "--loadmodule", "build/src/libmember.so", "--port", "6370"])
    request.addfinalizer(member1.kill)
    member2 = subprocess.Popen(["redis/src/redis-server", "--loadmodule", "build/src/libmember.so", "--port", "6371"])
    request.addfinalizer(member2.kill)
    time.sleep(2.0)
    master_client = redis.StrictRedis("127.0.0.1", 6369)
    master_client.execute_command("MASTER.ADD", "127.0.0.1", "6370")
    master_client.execute_command("MASTER.ADD", "127.0.0.1", "6371")

def test_ack():
    head_client = redis.StrictRedis("127.0.0.1", 6370)
    tail_client = redis.StrictRedis("127.0.0.1", 6371)
    # The ack client needs to be separate, since subscriptions
    # are blocking
    ack_client = redis.StrictRedis("127.0.0.1", 6371)
    p = ack_client.pubsub(ignore_subscribe_messages=True)
    p.subscribe("answers")
    time.sleep(0.5)
    p.get_message()
    ssn = head_client.execute_command("MEMBER.PUT", "task_spec", "some_random_value")
    time.sleep(0.5)
    put_ack = p.get_message()
    assert int(put_ack["data"]) == ssn # Check the sequence number
