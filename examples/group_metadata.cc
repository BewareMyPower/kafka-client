#include <arpa/inet.h>
#include <sstream>
#include <string>
#include <vector>
#include "rdkafka.hpp"

using namespace rdkafka;

static void parseMetadata(const void* data, int size);
static void parseAssignment(const void* data, int size);

int main(int argc, char* argv[]) {
  if (argc < 2 || strcmp(argv[1], "-h") == 0 ||
      strcmp(argv[1], "--help") == 0) {
    fprintf(stderr, "Usage: %s <group> [config-path]\n", argv[0]);
    return 1;
  }

  const char* group = argv[1];
  std::string configpath = (argc > 2) ? argv[2] : "config/consumer.conf";
  auto configs = readConfig(configpath);
  configs.first.setDefaultTopicConf(std::move(configs.second));

  char errstr[512];

  Consumer consumer(std::move(configs.first), errstr);
  if (consumer.isNull()) {
    fprintf(stderr, "Failed to create consumer: %s\n", errstr);
    return 2;
  }

  const struct rd_kafka_group_list* grplist;
  auto err = rd_kafka_list_groups(consumer.get(), group, &grplist, 5000);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    fprintf(stderr, "Failed to list groups: %s\n", errstr);
    return 3;
  }

  for (int i = 0; i < grplist->group_cnt; i++) {
    const rd_kafka_group_info& group = grplist->groups[i];
    if (group.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      fprintf(stderr, "group: %s err: %d (%s)\n", group.group, group.err,
              rd_kafka_err2str(group.err));
      continue;
    }

    printf("# group[%d]\n", i);
    for (int j = 0; j < group.member_cnt; j++) {
      printf("## member[%d]\n", j);
      const rd_kafka_group_member_info& member = group.members[j];
      printf("id: %s\n", member.member_id);
      printf("client id: %s\n", member.client_id);
      printf("host: %s\n", member.client_host);
      parseMetadata(member.member_metadata, member.member_metadata_size);
      parseAssignment(member.member_assignment, member.member_assignment_size);
    }
  }

  if (grplist) rd_kafka_group_list_destroy(grplist);

  return 0;
}

struct ByteBuffer {
  const void* data;
  int size;

  const char* char_ptr(int pos = 0) const noexcept {
    return static_cast<const char*>(data) + pos;
  }

  void dump() const {
    constexpr int kBytesPerLine = 8;
    for (int i = 0; i < size; i++) {
      printf("%02X", static_cast<uint8_t>(getInt8(i)));
      if ((i % kBytesPerLine < (kBytesPerLine - 1)) && i != (size - 1)) {
        printf(" ");
      } else {
        printf("\n");
      }
    }
  }

  void checkRange(int pos, int n) const {
    if (pos + n > size) {
      fprintf(stderr, "checkRange error: pos=%d n=%d size=%d\n", pos, n, size);
      abort();
    }
  }

  int8_t getInt8(int pos) const noexcept {
    checkRange(pos, 1);
    return *char_ptr(pos);
  }

  int16_t getInt16(int pos) const noexcept {
    checkRange(pos, 2);
    return ntohs(*reinterpret_cast<const uint16_t*>(char_ptr(pos)));
  }

  int32_t getInt32(int pos) const noexcept {
    checkRange(pos, 4);
    return ntohl(*reinterpret_cast<const uint32_t*>(char_ptr(pos)));
  }

  std::string getString(int pos) const noexcept {
    int16_t length = getInt16(pos);
    checkRange(pos + 2, length);
    return {char_ptr(pos + 2), static_cast<size_t>(length)};
  }
};

static void parseMetadata(const void* data, int size) {
  printf("metadata:\n");
  ByteBuffer buffer{data, size};
  buffer.dump();

  int version = buffer.getInt16(0);
  int topic_cnt = buffer.getInt32(2);
  printf("version: %d\ntopic count: %d\n", version, topic_cnt);

  int pos = 6;
  for (int i = 0; i < topic_cnt; i++) {
    auto topic = buffer.getString(pos);
    printf("topic[%d] = %s\n", i, topic.c_str());
    pos += (2 + topic.size());
  }
}

inline std::string to_string(const std::vector<int32_t>& partitions) {
  std::ostringstream stream;
  for (size_t i = 0; i < partitions.size(); i++) {
    if (i > 0) stream << ",";
    stream << partitions[i];
  }
  return stream.str();
}

static void parseAssignment(const void* data, int size) {
  printf("assignment:\n");
  ByteBuffer buffer{data, size};
  buffer.dump();

  int version = buffer.getInt16(0);
  int assign_cnt = buffer.getInt32(2);
  printf("version: %d\nassign count: %d\n", version, assign_cnt);

  int pos = 6;
  for (int i = 0; i < assign_cnt; i++) {
    auto topic = buffer.getString(pos);
    pos += (2 + topic.size());

    std::vector<int32_t> partitions;
    int partition_cnt = buffer.getInt32(pos);
    pos += 4;
    for (int j = 0; j < partition_cnt; j++) {
      partitions.emplace_back(buffer.getInt32(pos));
      pos += 4;
    }

    printf("Assign [%d]: topic=\"%s\" partitions=[%s]\n", i, topic.c_str(),
           to_string(partitions).c_str());
  }
}
