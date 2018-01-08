#include <chrono>

#include "glog/logging.h"

#include "client.h"

int num_completed = 0;

void AckCallback(redisAsyncContext* c, void* r, void* privdata) {
  const redisReply* reply = reinterpret_cast<redisReply*>(r);

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
  // CHECK(reply->type == REDIS_REPLY_ARRAY);
  // CHECK(reply->elements == 3);
  const redisReply* message_type = reply->element[0];
  if (strcmp(message_type->str, "message") == 0) {
    ++num_completed;
  } else if (strcmp(message_type->str, "subscribe") == 0) {
  } else {
    CHECK(false) << message_type->str;
  }
}

int main() {
  aeEventLoop* loop = aeCreateEventLoop(1024);
  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);
  client.RegisterAckCallback(&AckCallback);

  int num_calls = 0;
  const int N = 500000;

  LOG(INFO) << "starting bench";
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < N; ++i) {
    const int64_t callback_index = RedisCallbackManager::instance().add(
        [loop, &num_calls](const std::string& unused_data) {
          ++num_calls;
          if (num_calls == N) {
            aeStop(loop);
          }
        });
    const std::string i_str = std::to_string(i);
    client.RunAsync("MEMBER.PUT", i_str, i_str.data(), i_str.size(),
                    callback_index);
  }
  LOG(INFO) << "starting loop";
  aeMain(loop);
  auto end = std::chrono::steady_clock::now();
  CHECK(num_calls == N);
  CHECK(num_completed == N)
      << "num_completed " << num_completed << " vs N " << N;

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, latency (us) " << latency_us * 1.0 / N << ", num "
            << N;

  aeDeleteEventLoop(loop);
}
