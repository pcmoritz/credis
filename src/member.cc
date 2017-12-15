#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <map>
#include <memory>
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

#include "glog/logging.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "master_client.h"
#include "utils.h"

const char* const kCheckpointPath =
    "/tmp/gcs_ckpt";  // TODO(zongheng): don't hardcode.
const char* const kCheckpointHeaderKey = "";
const char* const kStringZero = "0";
const char* const kStringOne = "1";

extern "C" {
aeEventLoop* getEventLoop();
}

using Status = leveldb::Status;  // So that it can be easily replaced.

namespace {

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

int HandleNonOk(RedisModuleCtx* ctx, Status s) {
  if (s.ok()) {
    return REDISMODULE_OK;
  }
  LOG(ERROR) << s.ToString();
  RedisModule_ReplyWithSimpleString(ctx, "ERR");
  return REDISMODULE_ERR;
}

}  // namespace

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

  Status ConnectToMaster(const std::string& address, int port) {
    return master_client_.Connect(address, port);
  }
  MasterClient& Master() { return master_client_; }

  Status OpenCheckpoint(leveldb::DB** db) {
    static leveldb::Options options;
    options.create_if_missing = true;
    return leveldb::DB::Open(options, kCheckpointPath, db);
  }

  void SetRole(ChainRole chain_role) { chain_role_ = chain_role; }
  ChainRole Role() const { return chain_role_; }
  constexpr const char* ChainRoleName() const {
    return Role() == ChainRole::kHead
               ? "HEAD"
               : (Role() == ChainRole::kMiddle ? "MIDDLE" : "TAIL");
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

 private:
  std::string prev_address_;
  std::string prev_port_;
  std::string next_address_;
  std::string next_port_;

  MasterClient master_client_;

  ChainRole chain_role_;
  // The previous node in the chain (or NULL if none)
  redisAsyncContext* parent_;
  // The next node in the chain (or NULL if none)
  redisAsyncContext* child_;

  // Largest sequence number seen so far.  Initialized to the special value -1,
  // which indicates no updates have been processed yet.
  int64_t sn_ = -1;

  // The sent list.
  std::set<int64_t> sent_;
  // For implementing checkpointing.
  std::map<int64_t, std::string> sn_to_key_;
};

RedisChainModule module;

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
        long long sn,
        bool is_flush) {
  RedisModuleKey* key = reinterpret_cast<RedisModuleKey*>(
      RedisModule_OpenKey(ctx, name, REDISMODULE_WRITE));
  std::string k = ReadString(name);
  // TODO(pcm): error checking

  // State maintenance.
  if (is_flush) {
    RedisModule_DeleteKey(key);
    module.sn_to_key().erase(sn);
    // The tail has the responsibility of updating the sn_flushed watermark.
    if (module.Role() == RedisChainModule::ChainRole::kTail) {
      // "sn + 1" is the next sn to be flushed.
      module.Master().SetWatermark(MasterClient::Watermark::kSnFlushed, sn + 1);
    }
  } else {
    RedisModule_StringSet(key, data);
    module.sn_to_key()[sn] = k;
    module.record_sn(static_cast<int64_t>(sn));
  }

  // Protocol.
  std::string seqnum = std::to_string(sn);
  if (module.Role() == RedisChainModule::ChainRole::kTail) {
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
    std::string v = ReadString(data);
    LOG(INFO) << "calling MemberPropagate_RedisCommand";
    redisReply* reply = reinterpret_cast<redisReply*>(redisAsyncCommand(
        module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b %s", k.data(),
        k.size(), v.data(), v.size(), seqnum.data(), seqnum.size(),
        is_flush ? kStringOne : kStringZero));
    LOG(INFO) << "Done";
    freeReplyObject(reply);
    module.sent().insert(sn);
  }
  RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

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
    module.SetRole(RedisChainModule::ChainRole::kHead);
  } else if (role == "middle") {
    module.SetRole(RedisChainModule::ChainRole::kMiddle);
  } else if (role == "tail") {
    module.SetRole(RedisChainModule::ChainRole::kTail);
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
        module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b %s", key.data(),
        key.size(), value, size, sn.data(), sn.size(),
        /*is_flush=*/kStringZero));
    freeReplyObject(reply);
  }

  LOG(INFO) << "Called SET_ROLE with role " << module.ChainRoleName()
            << " and addresses " << prev_address << ":" << prev_port << " and "
            << next_address << ":" << next_port;

  RedisModule_ReplyWithLongLong(ctx, module.sn());
  return REDISMODULE_OK;
}

int MemberConnectToMaster_RedisCommand(RedisModuleCtx* ctx,
                                       RedisModuleString** argv,
                                       int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t size = 0;
  const char* ptr = RedisModule_StringPtrLen(argv[1], &size);
  long long port = 0;
  RedisModule_StringToLongLong(argv[2], &port);
  Status s = module.ConnectToMaster(std::string(ptr, size), port);
  if (!s.ok()) return RedisModule_ReplyWithError(ctx, s.ToString().data());
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
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
  if (module.Role() == RedisChainModule::ChainRole::kHead) {
    long long sn = module.inc_sn();
    return Put(ctx, argv[1], argv[2], sn, /*is_flush=*/false);
  } else {
    return RedisModule_ReplyWithError(ctx, "ERR called PUT on non-head node");
  }
}

// Propagate a put request down the chain
// argv[1] is the key for the data
// argv[2] is the data
// argv[3] is the sequence number for this update request
// argv[4] is a long long, either 0 or 1, indicating "is_flush".
int MemberPropagate_RedisCommand(RedisModuleCtx* ctx,
                                 RedisModuleString** argv,
                                 int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  long long sn = -1, is_flush = 0;
  RedisModule_StringToLongLong(argv[3], &sn);
  RedisModule_StringToLongLong(argv[4], &is_flush);
  return Put(ctx, argv[1], argv[2], sn, is_flush == 0 ? false : true);
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
  if (module.Role() != RedisChainModule::ChainRole::kTail) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the tail.");
  }

  // TODO(zongheng): the following checkpoints the range [sn_ckpt, sn_latest],
  // but any smaller chunk is valid (and perhaps desirable, when we want to keep
  // this redis command running < 1ms, say.)
  int64_t sn_ckpt = 0;
  Status s =
      module.Master().GetWatermark(MasterClient::Watermark::kSnCkpt, &sn_ckpt);
  HandleNonOk(ctx, s);
  const int64_t sn_latest = module.sn();

  // Fill in "redis key -> redis value".
  std::vector<std::string> keys_to_write;
  std::vector<std::string> vals_to_write;
  const auto& sn_to_key = module.sn_to_key();
  size_t size = 0;
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

  // Actually write this out.
  leveldb::WriteBatch batch;
  for (size_t i = 0; i < keys_to_write.size(); ++i) {
    batch.Put(keys_to_write[i], vals_to_write[i]);
  }
  // Open the checkpoint.
  leveldb::DB* ckpt;
  s = module.OpenCheckpoint(&ckpt);
  HandleNonOk(ctx, s);
  std::unique_ptr<leveldb::DB> ptr(ckpt);  // RAII.
  // TODO(zongheng): tune WriteOptions (sync, compression, etc).
  s = ckpt->Write(leveldb::WriteOptions(), &batch);
  HandleNonOk(ctx, s);

  s = module.Master().SetWatermark(MasterClient::Watermark::kSnCkpt,
                                   sn_latest + 1);
  HandleNonOk(ctx, s);

  const int64_t num_checkpointed = sn_latest - sn_ckpt + 1;
  return RedisModule_ReplyWithLongLong(ctx, num_checkpointed);
}

// HEAD.FLUSH: incrementally flush checkpointed entries out of memory.
//
// TODO(zongheng): this prototype assumes versioning is implemented.
//
// Errors out if not called on the head.
int HeadFlush_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {  // No arg needed.
    return RedisModule_WrongArity(ctx);
  }
  if (module.Role() != RedisChainModule::ChainRole::kHead) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the head.");
  }

  // Read watermarks from master.
  // Clearly, we can provide a batch iface.
  int64_t sn_ckpt = 0, sn_flushed = 0;
  HandleNonOk(ctx, module.Master().GetWatermark(
                       MasterClient::Watermark::kSnCkpt, &sn_ckpt));
  HandleNonOk(ctx, module.Master().GetWatermark(
                       MasterClient::Watermark::kSnFlushed, &sn_flushed));
  LOG(INFO) << "sn_flushed " << sn_flushed << ", sn_ckpt " << sn_ckpt;

  // TODO(zongheng): is this even correct?  who/when will sn_flushed be changed?
  // someone needs to subscribe to tail notif.

  // Any prefix of the range [sn_flushed, sn_ckpt) can be flushed.  Here we
  // flush 1 next entry.
  const auto& sn_to_key = module.sn_to_key();
  if (sn_flushed < sn_ckpt) {
    const auto it = sn_to_key.find(sn_flushed);
    CHECK(it != sn_to_key.end());
    RedisModuleString* key =
        RedisModule_CreateString(ctx, it->second.data(), it->second.size());
    int reply = Put(ctx, key, /*data=*/NULL,
                    sn_flushed,  // original sn that introduced this key
                    /*is_flush=*/true);
    // TODO(zongheng): probably need to check error.
    RedisModule_FreeString(ctx, key);
    return reply;
  }
  // sn_ckpt has not been incremented, so no new data can be flushed yet.
  return RedisModule_ReplyWithSimpleString(ctx, "Nothing to flush");
}

// LIST.CHECKPOINT: print to stdout all keys, values in the checkpoint file.
//
// For debugging.
int ListCheckpoint_RedisCommand(RedisModuleCtx* ctx,
                                RedisModuleString** argv,
                                int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {  // No arg needed.
    return RedisModule_WrongArity(ctx);
  }
  leveldb::DB* ckpt;
  Status s = module.OpenCheckpoint(&ckpt);
  HandleNonOk(ctx, s);
  std::unique_ptr<leveldb::DB> ptr(ckpt);  // RAII.
  LOG(INFO) << "-- LIST.CHECKPOINT:";
  leveldb::Iterator* it = ckpt->NewIterator(leveldb::ReadOptions());
  std::unique_ptr<leveldb::Iterator> iptr(it);  // RAII.
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (it->key().ToString() == kCheckpointHeaderKey) {
      // Let's skip the special header for prettier printing.
      continue;
    }
    LOG(INFO) << it->key().ToString() << ": " << it->value().ToString();
  }
  LOG(INFO) << "-- Done.";
  HandleNonOk(ctx, it->status());
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// READ: like redis' own GET, but can fall back to checkpoint file.
int Read_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  if (module.Role() != RedisChainModule::ChainRole::kTail) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the tail.");
  }

  KeyReader reader(ctx, argv[1]);
  if (!reader.IsEmpty()) {
    size_t size = 0;
    const char* value = reader.value(&size);
    return RedisModule_ReplyWithStringBuffer(ctx, value, size);
  } else {
    // Fall back to checkpoint file.
    leveldb::DB* ckpt;
    Status s = module.OpenCheckpoint(&ckpt);
    HandleNonOk(ctx, s);
    std::unique_ptr<leveldb::DB> ptr(ckpt);  // RAII.

    size_t size = 0;
    const char* key = reader.key(&size);
    std::string value;
    s = ckpt->Get(leveldb::ReadOptions(), leveldb::Slice(key, size), &value);
    if (s.IsNotFound()) {
      return RedisModule_ReplyWithNull(ctx);
    }
    HandleNonOk(ctx, s);
    return RedisModule_ReplyWithStringBuffer(ctx, value.data(), value.size());
  }
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
  if (RedisModule_CreateCommand(ctx, "MEMBER.CONNECT_TO_MASTER",
                                MemberConnectToMaster_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // TODO(zongheng): This should be renamed: it's only ever called on head.
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

  if (RedisModule_CreateCommand(
          ctx, "READ", Read_RedisCommand,
          "readonly",  // TODO(zongheng): is this ok?  It opens the db.
          /*firstkey=*/-1, /*lastkey=*/-1, /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Checkpointing & flushing.
  if (RedisModule_CreateCommand(
          ctx, "TAIL.CHECKPOINT", TailCheckpoint_RedisCommand, "write",
          /*firstkey=*/-1, /*lastkey=*/-1, /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(
          ctx, "HEAD.FLUSH", HeadFlush_RedisCommand, "write",
          /*firstkey=*/-1, /*lastkey=*/-1, /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Debugging only.
  if (RedisModule_CreateCommand(ctx, "LIST.CHECKPOINT",
                                ListCheckpoint_RedisCommand, "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MEMBER.SN", MemberSn_RedisCommand,
                                "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
}
