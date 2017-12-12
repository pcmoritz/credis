#ifndef CREDIS_UTILS_H
#define CREDIS_UTILS_H

// Convert RedisModuleString to C++ string
std::string ReadString(RedisModuleString* str) {
  size_t l;
  const char* s = RedisModule_StringPtrLen(str, &l);
  return std::string(s, l);
}

// Helper class to read data from a key and handle closing the key
// in an appropriate way.
class KeyReader {
 public:
  KeyReader(RedisModuleCtx* ctx, const std::string& key) : ctx_(ctx) {
    name_ = RedisModule_CreateString(ctx, key.data(), key.size());
    key_ = reinterpret_cast<RedisModuleKey*>(
        RedisModule_OpenKey(ctx, name_, REDISMODULE_READ));
  }

  KeyReader(RedisModuleCtx* ctx, RedisModuleString* key)
      : KeyReader(ctx, ReadString(key)) {}

  ~KeyReader() {
    RedisModule_CloseKey(key_);
    RedisModule_FreeString(ctx_, name_);
  }
  const char* key(size_t* size) {
    return RedisModule_StringPtrLen(name_, size);
  }
  const char* value(size_t* size) const {
    return RedisModule_StringDMA(key_, size, REDISMODULE_READ);
  }
  bool IsEmpty() const {
    return RedisModule_KeyType(key_) == REDISMODULE_KEYTYPE_EMPTY;
  }

 private:
  RedisModuleCtx* ctx_;
  RedisModuleString* name_;
  RedisModuleKey* key_;
};

#endif  // CREDIS_UTILS_H
