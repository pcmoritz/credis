#include "client.h"
#include "glog/logging.h"

int main() {
  aeEventLoop* loop = aeCreateEventLoop(1024);
  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);
  std::string data = "data";
  int num_calls = 0;
  const int N = 500000;
  for (int i = 0; i < N; ++i) {
    const int64_t callback_index = RedisCallbackManager::instance().add(
        [loop, &num_calls](const std::string& data) {
          num_calls += 1;
          if (num_calls == N) {
            aeStop(loop);
          }
        });
    const std::string i_str = std::to_string(i);
    client.RunAsync("MEMBER.PUT", i_str, i_str.data(), i_str.size(),
                    callback_index);
  }
  aeMain(loop);
  CHECK(num_calls == N);
  aeDeleteEventLoop(loop);
}
