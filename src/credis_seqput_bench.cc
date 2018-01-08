#include <chrono>
#include <vector>

#include "glog/logging.h"

#include "client.h"

const int N = 500000;
int num_completed = 0;
aeEventLoop* loop = aeCreateEventLoop(1024);

void SeqPutAckCallback(redisAsyncContext* context, void* r, void* privdata) {
  /* Replies to the SUBSCRIBE command have 3 elements. There are two
   * possibilities. Either the reply is the initial acknowledgment of the
   * subscribe command, or it is a message. If it is the initial acknowledgment,
   * then
   *     - reply->element[0]->str is "subscribe"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is null.
   * If it is an actual message, then
   *     - reply->element[0]->str is "message"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is the contents of the message.
   */
  if (r == nullptr) {
    LOG(INFO) << "null reply received";
    return;
  }
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  CHECK(reply->type == REDIS_REPLY_ARRAY);
  CHECK(reply->elements == 3);
  const redisReply* message_type = reply->element[0];
  LOG(INFO) << message_type->str;

  if (strcmp(message_type->str, "message") == 0) {
    // const int seqnum = std::stoi(reply->element[2]->str);
    // latencies[seqnum] =
    //     std::chrono::duration<float, std::micro>(now - time_starts[seqnum])
    //         .count();
    ++num_completed;
    if (num_completed == N) {
      aeStop(loop);
      return;
    }

    // Launch next pair.
    LOG(INFO) << "launching i = " << num_completed;
    const std::string s = std::to_string(num_completed);
    const int status =
        redisAsyncCommand(context, /*callback=*/NULL,
                          /*privdata=*/NULL, "MEMBER.SET %b %b", s.data(),
                          s.size(), s.data(), s.size());
    CHECK(status == REDIS_OK);
    LOG(INFO) << "launched";
  };
  LOG(INFO) << "returning from callback";
}

int main() {
  RedisClient client;
  CHECK(client.Connect("127.0.0.1", 6370).ok());
  CHECK(client.AttachToEventLoop(loop).ok());
  CHECK(client
            .RegisterAckCallback(
                static_cast<redisCallbackFn*>(&SeqPutAckCallback))
            .ok());
  redisAsyncContext* context = client.async_context();

  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // SeqPut.  Start with "0->0", and each callback will launch the next pair.
  const std::string kZeroStr = "0";
  const int status =
      redisAsyncCommand(context, /*callback=*/NULL,
                        /*privdata=*/NULL, "MEMBER.PUT %b %b", kZeroStr.data(),
                        kZeroStr.size(), kZeroStr.data(), kZeroStr.size());
  CHECK(status == REDIS_OK);

  LOG(INFO) << "start loop";
  aeMain(loop);
  LOG(INFO) << "end loop";

  auto end = std::chrono::system_clock::now();
  CHECK(num_completed == N)
      << "num_completed " << num_completed << " vs N " << N;
  LOG(INFO) << "ending bench";

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, total duration (ms) " << latency_us / 1e3 << ", num "
            << N;

  return 0;
}
