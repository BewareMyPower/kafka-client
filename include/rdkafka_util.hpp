// rdkafka_util.h: librdkafka function wrapper with error handler
#pragma once

#include "rdkafka.h"

#include <stdio.h>
#include <stdlib.h>

namespace rdkafka {

namespace util {

inline const char* getConfStr(rd_kafka_conf_t*) {
  return "global configuration";
}
inline const char* getConfStr(rd_kafka_topic_conf_t*) {
  return "topic configuration";
}

inline decltype(rd_kafka_conf_set)* getConfSetFunc(rd_kafka_conf_t*) {
  return rd_kafka_conf_set;
}
inline decltype(rd_kafka_topic_conf_set)* getConfSetFunc(
    rd_kafka_topic_conf_t*) {
  return rd_kafka_topic_conf_set;
}

template <typename T>
inline void putNameValue(T* conf, const char* name, const char* value) {
  char errstr[512];
  auto res = getConfSetFunc(conf)(conf, name, value, errstr, sizeof(errstr));
  if (res != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "%s [%s=%s] error: %s\n", getConfStr(conf), name, value,
            errstr);
    exit(1);
  }
}

inline void printPartitionList(
    const rd_kafka_topic_partition_list_t* partitions, FILE* fp = stdout) {
  for (int i = 0; i < partitions->cnt; ++i) {
    auto& elem = partitions->elems[i];
    fprintf(fp, "%s %s [%d] offset %lld", i > 0 ? "," : "", elem.topic,
            elem.partition, (long long)elem.offset);
  }
  fprintf(fp, "\n");
}

}  // namespace util

}  // namespace rdkafka
