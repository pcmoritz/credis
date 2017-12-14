#include "utils.h"

std::string ReadString(RedisModuleString* str) {
  size_t l = 0;
  const char* s = RedisModule_StringPtrLen(str, &l);
  return std::string(s, l);
}

KeyReader::KeyReader(RedisModuleCtx* ctx, const std::string& key) : ctx_(ctx) {
  name_ = RedisModule_CreateString(ctx, key.data(), key.size());
  key_ = reinterpret_cast<RedisModuleKey*>(
      RedisModule_OpenKey(ctx, name_, REDISMODULE_READ));
}

KeyReader::KeyReader(RedisModuleCtx* ctx, RedisModuleString* key)
    : KeyReader(ctx, ReadString(key)) {}

KeyReader::~KeyReader() {
  RedisModule_CloseKey(key_);
  RedisModule_FreeString(ctx_, name_);
}
const char* KeyReader::key(size_t* size) {
  return RedisModule_StringPtrLen(name_, size);
}
const char* KeyReader::value(size_t* size) const {
  return RedisModule_StringDMA(key_, size, REDISMODULE_READ);
}
bool KeyReader::IsEmpty() const {
  return RedisModule_KeyType(key_) == REDISMODULE_KEYTYPE_EMPTY;
}
