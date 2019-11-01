#include "rdkafka.hpp"
using namespace rdkafka;

class PartitionListGuard {
  rd_kafka_topic_partition_list_t* list_;

 public:
  PartitionListGuard(decltype(list_) lst) : list_(lst) {}
  ~PartitionListGuard() {
    if (list_) rd_kafka_topic_partition_list_destroy(list_);
  }
};

int main(int argc, char* argv[]) {
  if (argc < 3 ||
      (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)) {
    fprintf(stderr, "Usage: %s <topic-name> <offset> [config-path]\n", argv[0]);
    exit(1);
  }
  const char* topic_name = argv[1];
  auto offset = static_cast<int64_t>(std::stoll(argv[2]));
  std::string configpath = (argc > 3) ? argv[3] : "config/consumer.conf";

  auto configs = readConfig(configpath);
  configs.first.setDefaultTopicConf(std::move(configs.second));

  char errstr[512];
  Consumer consumer(std::move(configs.first), errstr);
  if (consumer.isNull())
    error::Exit("[INFO] Create consumer failed: %s\n", errstr);

  auto offsets = rd_kafka_topic_partition_list_new(1);
  PartitionListGuard offsets_guard(offsets);

  constexpr int kPartitionCount = 3;
  rd_kafka_topic_partition_list_add_range(offsets, topic_name, 0,
                                          kPartitionCount - 1);

  // get commited offsets
  {
    auto err = rd_kafka_committed(consumer.get(), offsets, 5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      fprintf(stderr, "rd_kafka_committed failed: %s\n", rd_kafka_err2str(err));
      exit(1);
    }
    printf("Current committed offsets:");
    util::printPartitionList(offsets);
  }

  // commit a large offset which is out of range

  for (int i = 0; i < kPartitionCount; i++)
    offsets->elems[i].offset = offset;

  {
    auto err = rd_kafka_commit(consumer.get(), offsets, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      fprintf(stderr, "commit failed: %s\n", rd_kafka_err2str(err));
      exit(1);
    }
    printf("Commit offsets:");
    util::printPartitionList(offsets);
  }

  return 0;
}
