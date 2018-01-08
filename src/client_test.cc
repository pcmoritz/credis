#include <chrono>

#include "glog/logging.h"

#include "client.h"

int main() {
  aeEventLoop* loop = aeCreateEventLoop(1024);
  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);

  int num_calls = 0;
  const int N = 500000;

  LOG(INFO) << "starting bench";
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < N; ++i) {
    const int64_t callback_index = RedisCallbackManager::instance().add(
        [loop, &num_calls](const std::string& data) {
          // LOG(INFO) << "'" << data << "'";
          // if (num_calls % 1000 == 0) {
          //   LOG(INFO) << "'" << data << "'";
          // }
          num_calls += 1;
          // if (num_calls == N) {
          //   aeStop(loop);
          // }
        });
    const std::string i_str = std::to_string(i);
    client.RunAsync("MEMBER.PUT", i_str, i_str.data(), i_str.size(),
                    callback_index);
  }
  LOG(INFO) << "starting loop";
  aeMain(loop);
  auto end = std::chrono::steady_clock::now();
  CHECK(num_calls == N);

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, latency (us) " << latency_us / N << ", num " << N;

  aeDeleteEventLoop(loop);
}
