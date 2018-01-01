#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "redismodule.h"
}

#include "glog/logging.h"

#include "utils.h"

struct Member {
  std::string address;
  std::string port;
  redisContext* context;
};

std::vector<Member> members;

long long SetRole(redisContext* context,
                  const std::string& role,
                  const std::string& prev_address,
                  const std::string& prev_port,
                  const std::string& next_address,
                  const std::string& next_port,
                  long long sn = -1) {
  std::string sn_string = std::to_string(sn);
  redisReply* reply = reinterpret_cast<redisReply*>(
      redisCommand(context, "MEMBER.SET_ROLE %s %s %s %s %s %s", role.c_str(),
                   prev_address.c_str(), prev_port.c_str(),
                   next_address.c_str(), next_port.c_str(), sn_string.c_str()));
  LOG(INFO) << "Last sequence number is " << reply->integer;
  long long sn_result = reply->integer;
  freeReplyObject(reply);
  return sn_result;
}

// Add a new replica to the chain
// argv[1] is the IP address of the replica to be added
// argv[2] is the port of the replica to be added
int MasterAdd_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  std::string address = ReadString(argv[1]);
  std::string port = ReadString(argv[2]);

  const size_t size = members.size();
  redisContext* context = SyncConnect(address, std::stoi(port));

  if (size == 0) {
    LOG(INFO) << "Node joined as a new head.";
  } else if (size == 1) {
    LOG(INFO) << "First tail joined. Now also connecting the first head.";
    Member head = members[0];
    SetRole(head.context, "head", "nil", "nil", address, port);
    SetRole(context, "tail", head.address, head.port, head.address, head.port);
  } else {
    LOG(INFO) << "New tail node joined.";
    LOG(INFO) << "Telling the old tail to be a middle node.";
    Member head = members[0];
    Member middle = members[size - 1];
    SetRole(middle.context, "middle", "", "", address, port);
    LOG(INFO) << "Replicating the tail.";
    redisReply* reply = reinterpret_cast<redisReply*>(
        redisCommand(middle.context, "MEMBER.REPLICATE"));
    freeReplyObject(reply);
    // TODO(pcm): Execute Sent_T requests
    LOG(INFO) << "Setting new tail.";
    SetRole(context, "tail", middle.address, middle.port, "nil", "nil");
  }
  Member tail;
  tail.address = address;
  tail.port = port;
  tail.context = context;
  members.emplace_back(tail);
  RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

// Remove a replica from the chain
// argv[1] is the IP address of the replica to be removed
// argv[2] is the port of the replica to be removed
int MasterRemove_RedisCommand(RedisModuleCtx* ctx,
                              RedisModuleString** argv,
                              int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  std::string address = ReadString(argv[1]);
  std::string port = ReadString(argv[2]);

  // Find the node to be removed
  size_t index = 0;
  do {
    if (members[index].address == address && members[index].port == port) {
      break;
    }
    index += 1;
  } while (index < members.size());

  if (index == members.size()) {
    return RedisModule_ReplyWithError(ctx, "replica not found");
  }

  if (index == members.size() - 1) {
    LOG(INFO) << "Removing the tail.";
    SetRole(members[index - 1].context, "tail", "nil", "nil",
            members[0].address, members[0].port);
  }

  members.erase(members.begin() + index);

  if (index == 0) {
    LOG(INFO) << "Removing the head.";
    SetRole(members[0].context, "head", "nil", "nil", members[1].address,
            members[1].port);
  }

  if (index != 0 && index != members.size()) {
    LOG(INFO) << "Removing the middle node " << index << ".";
    long long sn =
        SetRole(members[index].context, "", members[index - 1].address,
                members[index - 1].port, "", "");
    SetRole(members[index - 1].context, "", "", "", members[index].address,
            members[index].port, sn);
  }

  RedisModule_ReplyWithNull(ctx);

  return REDISMODULE_OK;
}

// MASTER.REFRESH_HEAD: return the current head if non-faulty, otherwise
// designate the child of the old head as the new head.
//
// Returns, as a string, "<new head addr>:<new head port>".
int MasterRefreshHead_RedisCommand(RedisModuleCtx* ctx,
                                   RedisModuleString** argv,
                                   int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }

  // RPC flow:
  // 1. master -> head: try to connect, check that if it's dead.
  // 2. (if dead) master cleans up state; then, master -> child of head:
  //    SetRole(head).
  // 3. (on ack) return from this function the new head.

  CHECK(!members.empty());
  redisContext* head = members[0].context;
  // (1).
  if (redisReconnect(head) == REDIS_OK) {
    // Current head is good.
    const std::string s = members[0].address + ":" + members[0].port;
    return RedisModule_ReplyWithSimpleString(ctx, s.data());
  }
  // (2).
  members.erase(members.begin());
  CHECK(members.size() >= 1 &&
        members.size() <= 2);  // TODO: implement adding a node?
  if (members.size() == 1) {
    SetRole(members[0].context, "head", "nil", "nil", "nil", "nil");
  } else {
    LOG(INFO) << "SetRole(head)";
    SetRole(members[0].context, "head", /*prev addr and port*/ "nil", "nil",
            /*re-use cached next addr and port*/ "", "");
  }
  // (3).
  const std::string s = members[0].address + ":" + members[0].port;
  return RedisModule_ReplyWithSimpleString(ctx, s.data());
}

extern "C" {

int RedisModule_OnLoad(RedisModuleCtx* ctx,
                       RedisModuleString** argv,
                       int argc) {
  REDISMODULE_NOT_USED(argc);
  REDISMODULE_NOT_USED(argv);

  if (RedisModule_Init(ctx, "MASTER", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.ADD", MasterAdd_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.REMOVE", MasterRemove_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.REFRESH_HEAD",
                                MasterRefreshHead_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
}
