#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

extern "C" {
#include "redismodule.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/ae.h"
}

extern "C" {
#include "redis/src/ae.h"
}

#include <iostream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "utils.h"

extern "C" {
aeEventLoop* getEventLoop();
}

redisAsyncContext* AsyncConnect(const std::string& address, int port) {
  redisAsyncContext* c = redisAsyncConnect(address.c_str(), port);
  if (c == NULL || c->err) {
    if (c) {
      printf("Connection error: %s\n", c->errstr);
      redisAsyncFree(c);
    } else {
      printf("Connection error: can't allocate redis context\n");
    }
    exit(1);
  }
  return c;
}

class RedisChainModule {
 public:
  enum ChainRole : int { HEAD = 0, MIDDLE = 1, TAIL = 2 };

  RedisChainModule() : sn_(0), child_(NULL) {}

  ~RedisChainModule() {
    if (child_) {
      redisAsyncFree(child_);
    }
  }

  redisAsyncContext* Reset(std::string& prev_address,
                           std::string& prev_port,
                           std::string& next_address,
                           std::string& next_port,
                           ChainRole chain_role) {
    prev_address_ = prev_address;
    prev_port_ = prev_port;
    next_address_ = next_address;
    next_port_ = next_port;
    chain_role_ = chain_role;

    if (child_) {
      redisAsyncFree(child_);
    }

    child_ = AsyncConnect(next_address, std::stoi(next_port));

    return child_;
  }

  std::string prev_address() { return prev_address_; }

  std::string prev_port() { return prev_port_; }

  std::string next_address() { return next_address_; }

  std::string next_port() { return next_port_; }

  int64_t next_sn() {
    std::cout << "sequence number is " << sn_ << std::endl;
    return sn_++;
  }

  ChainRole chain_role() { return chain_role_; }

  redisAsyncContext* child() { return child_; }

  void Put(int64_t sn, const std::string& key) {
    std::cout << "added sequence number " << sn << std::endl;
    sn_to_key_[sn] = key;
  }

  std::unordered_map<int64_t, std::string>& sn_to_key() { return sn_to_key_; }

 private:
  std::string prev_address_;
  std::string prev_port_;
  std::string next_address_;
  std::string next_port_;
  ChainRole chain_role_;
  int64_t sn_;
  redisAsyncContext* child_;

  // The sent list
  std::set<int64_t> sent_;
  // For implementing flushing
  std::unordered_map<int64_t, std::string> sn_to_key_;
};

RedisChainModule module;

// Set the role (head, middle, tail), successor and predecessor of this server
// argv[1] is the role of this instance ("head", "middle", "tail")
// argv[2] is the address of the previous node in the chain
// argv[3] is the port of the previous node in the chain
// argv[4] is the address of the next node in the chain
// argv[5] is the port of the next node in the chain
int MemberSetRole_RedisCommand(RedisModuleCtx* ctx,
                               RedisModuleString** argv,
                               int argc) {
  if (argc != 6) {
    return RedisModule_WrongArity(ctx);
  }
  std::string role = ReadString(argv[1]);
  RedisChainModule::ChainRole chain_role;
  if (role == "head") {
    chain_role = RedisChainModule::ChainRole::HEAD;
  } else if (role == "middle") {
    chain_role = RedisChainModule::ChainRole::MIDDLE;
  } else {
    assert(role == "tail");
    chain_role = RedisChainModule::ChainRole::TAIL;
  }

  std::string prev_address = ReadString(argv[2]);
  if (prev_address == "nil") {
    prev_address = module.prev_address();
  }
  std::string prev_port = ReadString(argv[3]);
  if (prev_port == "nil") {
    prev_port = module.prev_port();
  }
  std::string next_address = ReadString(argv[4]);
  if (next_address == "nil") {
    next_address = module.next_address();
  }
  std::string next_port = ReadString(argv[5]);
  if (next_port == "nil") {
    next_port = module.next_port();
  }

  std::cout << "Called SET_ROLE with " << prev_address << ":" << prev_port
            << " and " << next_address << ":" << next_port << std::endl;

  redisAsyncContext* c = module.Reset(prev_address, prev_port, next_address,
                                      next_port, chain_role);

  aeEventLoop* loop = getEventLoop();
  redisAeAttach(loop, c);

  RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

int Put(RedisModuleCtx* ctx,
        RedisModuleString* name,
        RedisModuleString* data,
        long long sn) {
  RedisModuleKey* key;
  key = reinterpret_cast<RedisModuleKey*>(
      RedisModule_OpenKey(ctx, name, REDISMODULE_WRITE));
  // TODO(pcm): error checking
  RedisModule_StringSet(key, data);
  std::string rid = std::to_string(sn);
  std::string k = ReadString(name);
  module.Put(sn, k);
  if (module.chain_role() == RedisChainModule::TAIL) {
    RedisModuleCallReply* reply =
        RedisModule_Call(ctx, "PUBLISH", "cc", "answers", rid.c_str());
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
      return RedisModule_ReplyWithCallReply(ctx, reply);
    }
  } else {
    std::string v = ReadString(data);
    redisReply* reply = reinterpret_cast<redisReply*>(redisAsyncCommand(
        module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b", k.data(),
        k.size(), v.data(), v.size(), rid.data(), rid.size()));
    freeReplyObject(reply);
  }
  RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

// Put a key. This is only called on the head node by the client.
// argv[1] is the key for the data
// argv[2] is the data
int MemberPut_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  if (module.chain_role() == RedisChainModule::HEAD) {
    long long sn = module.next_sn();
    return Put(ctx, argv[1], argv[2], sn);
  } else {
    return RedisModule_ReplyWithError(ctx, "called PUT on non head node");
  }
}

// Propagate a put request down the chain
// argv[1] is the key for the data
// argv[2] is the data
// argv[3] is the request ID
int MemberPropagate_RedisCommand(RedisModuleCtx* ctx,
                                 RedisModuleString** argv,
                                 int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  long long sn;
  RedisModule_StringToLongLong(argv[3], &sn);
  return Put(ctx, argv[1], argv[2], sn);
}

// Replicate our content to our child
int MemberReplicate_RedisCommand(RedisModuleCtx* ctx,
                                 RedisModuleString** argv,
                                 int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  std::cout << "Called replicate." << std::endl;
  for (auto element : module.sn_to_key()) {
    KeyReader reader(ctx, element.second);
    size_t key_size, value_size;
    const char* key_data = reader.key(&key_size);
    const char* value_data = reader.value(&value_size);
    redisReply* reply = reinterpret_cast<redisReply*>(
        redisAsyncCommand(module.child(), NULL, NULL, "SET %b %b", key_data,
                          key_size, value_data, value_size));
    freeReplyObject(reply);
  }
  std::cout << "Done replicating." << std::endl;
  RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

extern "C" {

int RedisModule_OnLoad(RedisModuleCtx* ctx,
                       RedisModuleString** argv,
                       int argc) {
  REDISMODULE_NOT_USED(argc);
  REDISMODULE_NOT_USED(argv);
  if (RedisModule_Init(ctx, "MEMBER", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "MEMBER.SET_ROLE",
                                MemberSetRole_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "MEMBER.PUT", MemberPut_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "MEMBER.PROPAGATE",
                                MemberPropagate_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "MEMBER.REPLICATE",
                                MemberReplicate_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
}
