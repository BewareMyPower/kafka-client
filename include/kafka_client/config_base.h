#ifndef KAFKA_CLIENT_CONFIG_BASE_H
#define KAFKA_CLIENT_CONFIG_BASE_H

#include "kafka_client/error_message.h"

#include <memory>
#include "librdkafka/rdkafka.h"

namespace kafka_client {

template <typename ConfT>
class ConfigBase {
 public:
  /**
   * @brief Put a key-value pair to config
   * @returns true or false on error
   */
  bool Put(const char* key, const char* value);

  /**
   * @brief Get \p key's associated value.
   * @returns The associated value string or an empty string on error
   */
  std::string Get(const char* key) const;

  /**
   * @brief Returns internal error message.
   *
   * If error occurs in other methods, call \c Error() to get error message
   * I.e.:
   * @code
   *   // assuming config is an object of ConfigBase
   *   if (!config.Put("bootstrap.servers", "localhost:9092"))
   *     fprintf(stderr, "Put failed: %s\n", config.Error());
   * @endcode
   */
  const char* Error() const noexcept { return error_.data(); }

  // NOTE: it's only used to transfer ownership of \c handle_
  ConfT* Detach() noexcept { return conf_.release(); }

 protected:
  using ConfDeleter = void (*)(ConfT*);

  explicit ConfigBase(ConfT* conf, ConfDeleter deleter)
      : conf_(conf, deleter) {}

  virtual rd_kafka_conf_res_t RdKafkaConfSet(const char* name,
                                             const char* value, char* errstr,
                                             size_t errstr_size) = 0;

  virtual rd_kafka_conf_res_t RdKafkaConfGet(const char* name, char* dest,
                                             size_t* dest_size) const = 0;

  virtual const char* name() const = 0;

  ConfT* handle() const noexcept { return conf_.get(); }

 private:
  std::unique_ptr<ConfT, ConfDeleter> conf_;
  mutable ErrorMessage error_;
};

template <typename ConfT>
inline bool ConfigBase<ConfT>::Put(const char* key, const char* value) {
  if (!key) {
    error_.Format("%s Put null key", name());
    return false;
  }
  if (!value) {
    error_.Format("%s Put \"%s\" => null", name(), key);
    return false;
  }

  char errstr[512];
  if (RdKafkaConfSet(key, value, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    error_.Format("%s Put \"%s\" => \"%s\": %s", name(), key, value, errstr);
    return false;
  }

  return true;
}

template <typename ConfT>
inline std::string ConfigBase<ConfT>::Get(const char* key) const {
  if (!key) {
    error_.Format("%s Get failed: null key", name());
    return "";
  }

  size_t value_size;
  if (RdKafkaConfGet(key, nullptr, &value_size) != RD_KAFKA_CONF_OK) {
    error_.Format("%s Get(\"%s\") failed", name(), key);
    return "";
  }

  std::string value(value_size, '\0');
  RdKafkaConfGet(key, &value[0], &value_size);

  if (!value.empty() && value.back() == '\0') value.pop_back();
  return value;
}

}  // namespace kafka_client

#endif  // CONFIG_BASE_H
