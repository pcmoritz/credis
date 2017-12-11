#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <map>
#include <set>
#include <string>
#include <vector>

extern "C" {
#include "hiredis/adapters/ae.h"
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "redismodule.h"
}

extern "C" {
#include "redis/src/ae.h"
}

#include <glog/logging.h>
#include "leveldb/db.h"

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
  enum class ChainRole : int {
    kHead = 0,
    kMiddle = 1,
    kTail = 2,
  };

  RedisChainModule()
      : chain_role_(ChainRole::kHead), parent_(NULL), child_(NULL) {}

  ~RedisChainModule() {
    if (child_) {
      redisAsyncFree(child_);
    }
  }

  void Reset(std::string& prev_address,
             std::string& prev_port,
             std::string& next_address,
             std::string& next_port) {
    prev_address_ = prev_address;
    prev_port_ = prev_port;
    next_address_ = next_address;
    next_port_ = next_port;

    if (child_) {
      redisAsyncFree(child_);
    }
    if (parent_) {
      redisAsyncFree(parent_);
    }
    if (next_address != "nil") {
      child_ = AsyncConnect(next_address, std::stoi(next_port));
    } else {
      child_ = NULL;
    }
    if (prev_address != "nil") {
      parent_ = AsyncConnect(prev_address, std::stoi(prev_port));
    } else {
      parent_ = NULL;
    }
  }

  void set_role(ChainRole chain_role) { chain_role_ = chain_role; }
  ChainRole chain_role() const { return chain_role_; }
  constexpr const char* ChainRoleName() const {
    return chain_role() == ChainRole::kHead
               ? "HEAD"
               : (chain_role() == ChainRole::kMiddle ? "MIDDLE" : "TAIL");
  }

  std::string prev_address() { return prev_address_; }
  std::string prev_port() { return prev_port_; }
  std::string next_address() { return next_address_; }
  std::string next_port() { return next_port_; }
  redisAsyncContext* child() { return child_; }
  redisAsyncContext* parent() { return parent_; }

  std::set<int64_t>& sent() { return sent_; }

  // Sequence numbers.
  std::map<int64_t, std::string>& sn_to_key() { return sn_to_key_; }
  int64_t sn() const { return sn_; }
  int64_t inc_sn() {
    CHECK(chain_role_ == ChainRole::kHead)
        << "Logical error?: only the head should increment the sn.";
    LOG(INFO) << "Using sequence number " << sn_ + 1;
    return ++sn_;
  }
  void record_sn(int64_t sn) { sn_ = std::max(sn_, sn); }

  void record_update(int64_t sn, const std::string& key) {
    LOG(INFO) << "added sequence number " << sn;
    sn_to_key_[sn] = key;
  }

  // TODO(zongheng): this field should be picked up from the ckpt file, instead
  // of being cached.
  int64_t sn_ckpt() const { return sn_ckpt_; }
  void set_sn_ckpt(int64_t sn) { sn_ckpt_ = sn; }

 private:
  std::string prev_address_;
  std::string prev_port_;
  std::string next_address_;
  std::string next_port_;

  ChainRole chain_role_;
  // The previous node in the chain (or NULL if none)
  redisAsyncContext* parent_;
  // The next node in the chain (or NULL if none)
  redisAsyncContext* child_;

  // Largest sequence number seen so far.  Initialized to the special value -1,
  // which indicates no updates have been processed yet.
  int64_t sn_ = -1;
  // Next sn to checkpoint.  The checkpointing algorithm is free to checkpoint
  // the range [sn_ckpt_, sn_].
  int64_t sn_ckpt_ = 0;

  // The sent list.
  std::set<int64_t> sent_;
  // For implementing checkpointing.
  std::map<int64_t, std::string> sn_to_key_;
};

RedisChainModule module;

// Set the role (head, middle, tail), successor and predecessor of this server.
// Each of the arguments can be the empty string, in which case it is not set.
// argv[1] is the role of this instance ("head", "middle", "tail")
// argv[2] is the address of the previous node in the chain
// argv[3] is the port of the previous node in the chain
// argv[4] is the address of the next node in the chain
// argv[5] is the port of the next node in the chain
// argv[6] is the last sequence number the next node did receive
// (on node removal) and -1 if no node is removed
// Returns the latest sequence number on this node
int MemberSetRole_RedisCommand(RedisModuleCtx* ctx,
                               RedisModuleString** argv,
                               int argc) {
  if (argc != 7) {
    return RedisModule_WrongArity(ctx);
  }
  std::string role = ReadString(argv[1]);
  if (role == "head") {
    module.set_role(RedisChainModule::ChainRole::kHead);
  } else if (role == "middle") {
    module.set_role(RedisChainModule::ChainRole::kMiddle);
  } else if (role == "tail") {
    module.set_role(RedisChainModule::ChainRole::kTail);
  } else {
    assert(role == "");
  }

  std::string prev_address = ReadString(argv[2]);
  if (prev_address == "") {
    prev_address = module.prev_address();
  }
  std::string prev_port = ReadString(argv[3]);
  if (prev_port == "") {
    prev_port = module.prev_port();
  }
  std::string next_address = ReadString(argv[4]);
  if (next_address == "") {
    next_address = module.next_address();
  }
  std::string next_port = ReadString(argv[5]);
  if (next_port == "") {
    next_port = module.next_port();
  }

  module.Reset(prev_address, prev_port, next_address, next_port);

  if (module.child()) {
    aeEventLoop* loop = getEventLoop();
    redisAeAttach(loop, module.child());
  }

  if (module.parent()) {
    aeEventLoop* loop = getEventLoop();
    redisAeAttach(loop, module.parent());
  }

  int64_t first_sn = std::stoi(ReadString(argv[6]));

  for (auto i = module.sn_to_key().find(first_sn);
       i != module.sn_to_key().end(); ++i) {
    std::string sn = std::to_string(i->first);
    std::string key = i->second;
    KeyReader reader(ctx, key);
    size_t size;
    const char* value = reader.value(&size);
    redisReply* reply = reinterpret_cast<redisReply*>(redisAsyncCommand(
        module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b", key.data(),
        key.size(), value, size, sn.data(), sn.size()));
    freeReplyObject(reply);
  }

  LOG(INFO) << "Called SET_ROLE with role " << module.ChainRoleName()
            << " and addresses " << prev_address << ":" << prev_port << " and "
            << next_address << ":" << next_port;

  RedisModule_ReplyWithLongLong(ctx, module.sn());
  return REDISMODULE_OK;
}

// Helper function to handle updates locally.
//
// For all nodes: (1) actual update into redis, (1) update the internal
// sn_to_key state.
//
// For non-tail nodes: propagate.
//
// For tail: publish that the request has been finalized, and ack up to the
// chain.
int Put(RedisModuleCtx* ctx,
        RedisModuleString* name,
        RedisModuleString* data,
        long long sn) {
  RedisModuleKey* key;
  key = reinterpret_cast<RedisModuleKey*>(
      RedisModule_OpenKey(ctx, name, REDISMODULE_WRITE));
  // TODO(pcm): error checking
  RedisModule_StringSet(key, data);
  std::string seqnum = std::to_string(sn);
  std::string k = ReadString(name);
  module.record_update(sn, k);
  if (module.chain_role() == RedisChainModule::ChainRole::kTail) {
    RedisModuleCallReply* reply =
        RedisModule_Call(ctx, "PUBLISH", "cc", "answers", seqnum.c_str());
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
      return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    reply = RedisModule_Call(ctx, "MEMBER.ACK", "c", seqnum.c_str());
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
      return RedisModule_ReplyWithCallReply(ctx, reply);
    }
  } else {
    // TODO(zongheng): is the ordering of sent-list insertion & actual command
    // launch important?
    std::string v = ReadString(data);
    redisReply* reply = reinterpret_cast<redisReply*>(redisAsyncCommand(
        module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b", k.data(),
        k.size(), v.data(), v.size(), seqnum.data(), seqnum.size()));
    freeReplyObject(reply);
    module.sent().insert(sn);
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
  if (module.chain_role() == RedisChainModule::ChainRole::kHead) {
    long long sn = module.inc_sn();
    return Put(ctx, argv[1], argv[2], sn);
  } else {
    return RedisModule_ReplyWithError(ctx, "ERR called PUT on non-head node");
  }
}

// Propagate a put request down the chain
// argv[1] is the key for the data
// argv[2] is the data
// argv[3] is the sequence number for this update request
int MemberPropagate_RedisCommand(RedisModuleCtx* ctx,
                                 RedisModuleString** argv,
                                 int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  long long sn;
  RedisModule_StringToLongLong(argv[3], &sn);
  module.record_sn(static_cast<int64_t>(sn));
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
  LOG(INFO) << "Called replicate.";
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
  LOG(INFO) << "Done replicating.";
  RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

// Send ack up the chain.
// This could be batched in the future if we want to.
// argv[1] is the sequence number that is acknowledged
int MemberAck_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  std::string sn = ReadString(argv[1]);
  LOG(INFO) << "Erasing sequence number " << sn << " from sent list";
  module.sent().erase(std::stoi(sn));
  if (module.parent()) {
    LOG(INFO) << "Propagating the ACK up the chain";
    redisReply* reply = reinterpret_cast<redisReply*>(redisAsyncCommand(
        module.parent(), NULL, NULL, "MEMBER.ACK %b", sn.data(), sn.size()));
    freeReplyObject(reply);
  }
  RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

// TAIL.CHECKPOINT: incrementally checkpoint in-memory entries to durable
// storage.
//
// Returns the number of sequence numbers newly checkpointed.  Errors out if not
// called on the tail.
int TailCheckpoint_RedisCommand(RedisModuleCtx* ctx,
                                RedisModuleString** argv,
                                int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {  // No arg needed.
    return RedisModule_WrongArity(ctx);
  }
  if (module.chain_role() != RedisChainModule::ChainRole::kTail) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the tail.");
  }

  // Fill in "redis key -> redis value".
  std::vector<std::string> keys_to_write;
  std::vector<std::string> vals_to_write;
  const auto& sn_to_key = module.sn_to_key();
  size_t size = 0;

  // TODO(zongheng): the following checkpoints the range [sn_ckpt_, sn_], but
  // any smaller chunk will do (and perhaps desirable, when we want to keep this
  // redis command running < 1ms, say.)
  const int64_t sn_latest = module.sn();
  const int64_t sn_ckpt = module.sn_ckpt();
  for (int64_t s = sn_ckpt; s <= sn_latest; ++s) {
    auto i = sn_to_key.find(s);
    CHECK(i != sn_to_key.end())
        << "ERR the sn_to_key map doesn't contain seqnum " << s;
    std::string key = i->second;
    const KeyReader reader(ctx, key);
    const char* value = reader.value(&size);

    keys_to_write.push_back(key);
    vals_to_write.emplace_back(std::string(value, size));
  }

  // TODO(zongheng): actually write this out.
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  LOG(INFO) << "leveldb " << status.ok();

  const int64_t checkpointed = sn_latest - sn_ckpt + 1;
  module.set_sn_ckpt(sn_latest + 1);
  return RedisModule_ReplyWithLongLong(ctx, checkpointed);
}

// MEMBER.SN: the largest SN processed by this node.
//
// For debugging.
int MemberSn_RedisCommand(RedisModuleCtx* ctx,
                          RedisModuleString** argv,
                          int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {  // No arg needed.
    return RedisModule_WrongArity(ctx);
  }
  return RedisModule_ReplyWithLongLong(ctx, module.sn());
}

extern "C" {

int RedisModule_OnLoad(RedisModuleCtx* ctx,
                       RedisModuleString** argv,
                       int argc) {
  REDISMODULE_NOT_USED(argc);
  REDISMODULE_NOT_USED(argv);
  if (RedisModule_Init(ctx, "MEMBER", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MEMBER.SET_ROLE",
                                MemberSetRole_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.PUT", MemberPut_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.PROPAGATE",
                                MemberPropagate_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.REPLICATE",
                                MemberReplicate_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.ACK", MemberAck_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // A special command only runnable on the tail.
  if (RedisModule_CreateCommand(
          ctx, "TAIL.CHECKPOINT", TailCheckpoint_RedisCommand, "write",
          /*firstkey=*/-1, /*lastkey=*/-1, /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Debugging only.
  if (RedisModule_CreateCommand(ctx, "MEMBER.SN", MemberSn_RedisCommand,
                                "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
}
