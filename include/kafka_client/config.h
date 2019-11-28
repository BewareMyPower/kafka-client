#ifndef KAFKA_CLIENT_CONFIG_H
#define KAFKA_CLIENT_CONFIG_H

#include "kafka_client/config_base.h"

namespace kafka_client {

class TopicConfig : public ConfigBase<rd_kafka_topic_conf_t> {
 public:
  using Base = ConfigBase<rd_kafka_topic_conf_t>;

  TopicConfig()
      : Base(rd_kafka_topic_conf_new(), &rd_kafka_topic_conf_destroy) {}

 private:
  rd_kafka_conf_res_t RdKafkaConfSet(const char* name, const char* value,
                                     char* errstr, size_t errstr_size) override;

  rd_kafka_conf_res_t RdKafkaConfGet(const char* name, char* dest,
                                     size_t* dest_size) const override;

  const char* name() const { return "TopicConfig"; }
};

class GlobalConfig : public ConfigBase<rd_kafka_conf_t> {
 public:
  using Base = ConfigBase<rd_kafka_conf_t>;

  GlobalConfig() : Base(rd_kafka_conf_new(), &rd_kafka_conf_destroy) {}

 private:
  rd_kafka_conf_res_t RdKafkaConfSet(const char* name, const char* value,
                                     char* errstr, size_t errstr_size) override;

  rd_kafka_conf_res_t RdKafkaConfGet(const char* name, char* dest,
                                     size_t* dest_size) const override;

  const char* name() const { return "GlobalConfig"; }
};

inline rd_kafka_conf_res_t TopicConfig::RdKafkaConfSet(const char* name,
                                                       const char* value,
                                                       char* errstr,
                                                       size_t errstr_size) {
  return rd_kafka_topic_conf_set(handle(), name, value, errstr, errstr_size);
}

inline rd_kafka_conf_res_t TopicConfig::RdKafkaConfGet(
    const char* name, char* dest, size_t* dest_size) const {
  return rd_kafka_topic_conf_get(handle(), name, dest, dest_size);
}

inline rd_kafka_conf_res_t GlobalConfig::RdKafkaConfSet(const char* name,
                                                        const char* value,
                                                        char* errstr,
                                                        size_t errstr_size) {
  return rd_kafka_conf_set(handle(), name, value, errstr, errstr_size);
}

inline rd_kafka_conf_res_t GlobalConfig::RdKafkaConfGet(
    const char* name, char* dest, size_t* dest_size) const {
  return rd_kafka_conf_get(handle(), name, dest, dest_size);
}

}  // namespace kafka_client

#endif  // KAFKA_CLIENT_CONFIG_H
