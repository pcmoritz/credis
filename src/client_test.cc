#include "client.h"



int main() {
  aeEventLoop *loop = aeCreateEventLoop(1024);
  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);
  std::string data = "data";
  int num_calls = 0;
  for (int i = 0; i < 1000; ++i) {
    int64_t callback_index = RedisCallbackManager::instance().add([loop, &num_calls](const std::string& data) {
      num_calls += 1;
      if(num_calls == 1000) {
        aeStop(loop);
      }
    });
    client.RunAsync("MEMBER.PUT", std::to_string(i), (uint8_t*) data.data(), data.size(), callback_index);
  }
  aeMain(loop);
  aeDeleteEventLoop(loop);
}
