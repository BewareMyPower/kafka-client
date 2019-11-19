#ifndef KAFKA_CLIENT_CONFIG_H
#define KAFKA_CLIENT_CONFIG_H

#include <memory>
#include "kafka_client/error_message.h"
#include "rdkafka.h"

namespace kafka_client {

class GlobalConfig {
 public:
  GlobalConfig() : handle_(rd_kafka_conf_new(), &rd_kafka_conf_destroy) {}

  const char* error() const noexcept { return error_.data(); }
  rd_kafka_conf_t* handle() const { return handle_.get(); }

  // Transfer ownership to user
  rd_kafka_conf_t* Release() noexcept { return handle_.release(); }

  // Returns false if failed
  bool Put(const char* key, const char* value);

  // Returns an empty string if failed
  std::string Get(const char* key) const;

 private:
  std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> handle_;
  mutable ErrorMessage error_;
};

#include <assert.h>

inline bool GlobalConfig::Put(const char* key, const char* value) {
  assert(key && value);
  char errstr[512];

  auto conf_res =
      rd_kafka_conf_set(handle(), key, value, errstr, sizeof(errstr));
  if (conf_res != RD_KAFKA_CONF_OK) {
    error_.Format("GlobalConfig::put key=\"%s\" value=\"%s\" failed: %s", key,
                  value, errstr);
    return false;
  }

  return true;
}

inline std::string GlobalConfig::Get(const char* key) const {
  assert(key);

  size_t value_size = 0;
  auto conf_res = rd_kafka_conf_get(handle(), key, nullptr, &value_size);
  if (conf_res != RD_KAFKA_CONF_OK) {
    error_.Format(
        "GlobalConfig::get key=\"%s\" failed: Unknown configuration name", key);
    return "";
  }

  std::string value(value_size, '\0');
  conf_res = rd_kafka_conf_get(handle(), key, &value[0], &value_size);
  if (conf_res != RD_KAFKA_CONF_OK) {
    error_.Format(
        "GlobalConfig::get key=\"%s\" failed: Unknown configuration name", key);
    return "";
  }

  assert(!value.empty());
  if (value.back() == '\0') value.pop_back();
  return value;
}

}  // namespace kafka_client

#endif  // KAFKA_CLIENT_CONFIG_H
