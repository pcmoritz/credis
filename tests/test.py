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
    head_client.execute_command("MEMBER.PUT", "task_spec", "some_random_value")
    p = ack_client.pubsub()
    p.subscribe("answers")
    put_ack = p.get_message()
    assert put_ack["data"] == 1 # Check the sequence number
