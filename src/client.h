#ifndef CREDIS_CLIENT_H_
#define CREDIS_CLIENT_H_

#include <stdlib.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
}

#include "glog/logging.h"
#include "leveldb/db.h"

using Status = leveldb::Status;

class RedisCallbackManager {
 public:
  using RedisCallback = std::function<void(const std::string &)>;

  static RedisCallbackManager &instance() {
    static RedisCallbackManager instance;
    return instance;
  }

  int64_t add(const RedisCallback &function);

  RedisCallback &get(int64_t callback_index);

 private:
  RedisCallbackManager() : num_callbacks(0){};

  ~RedisCallbackManager() { }

  int64_t num_callbacks;
  std::unordered_map<int64_t, std::unique_ptr<RedisCallback>> callbacks_;
};

class RedisClient {
 public:
  RedisClient() {}
  ~RedisClient();
  Status Connect(const std::string &address, int port);
  Status AttachToEventLoop(aeEventLoop *loop);
  Status RunAsync(const std::string &command,
                  const std::string &id,
                  uint8_t *data,
                  int64_t length,
                  int64_t callback_index);

 private:
  redisContext *context_;
  redisAsyncContext *async_context_;
};

#endif  // CREDIS_CLIENT_H_