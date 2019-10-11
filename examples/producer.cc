// producer.cc
#include <signal.h>
#include <unistd.h>
#include "rdkafka.hpp"
using namespace rdkafka;

static bool run = true;
static long num_message = 0;
static long num_message_left = 0;

static void dr_msg_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage,
                      void* opaque);

int main(int argc, char* argv[]) {
  // command line config
  if (argc > 1 &&
      (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0))
    error::Exit("Usage: %s [topic-name] [config-path]\n", argv[0]);

  const char* topic_name = (argc > 1) ? argv[1] : "xyz-topic";
  std::string configpath = (argc > 2) ? argv[2] : "config/producer.conf";

  // if stdin is redirected to file, is_terminal is 0, then
  // some interaction message won't be printed
  bool is_terminal = isatty(STDIN_FILENO) == 1;

  // file config
  error::Print("[INFO] Read configuration from %s\n", configpath.data());
  auto configs = readConfig(configpath);
  configs.first.setDeliveryReportCallback(dr_msg_cb);  // global conf

  // shared error message
  char errstr[512];
  ErrorCode error_code;

  // create producer from global config
  Producer producer(std::move(configs.first), errstr);
  if (producer.isNull())
    error::Exit("[ERROR] Create producer failed: %s\n", errstr);
  error::Print("[INFO] Create producer: %s\n", producer.getName());

  // create topic from topic config, Producer::produce() method need it
  Topic topic(producer.get(), topic_name, std::move(configs.second));

  signal(SIGINT, [](int) {
    if (!run) exit(1);  // already break the loop, force to exit
    // break the loop, close() is to let blocked fgets() return nullptr
    run = false;
    close(STDIN_FILENO);
  });
  if (is_terminal)
    error::Print("[INFO] Enter line to send (Press Ctrl+C to safe exit)\n");

  // loop: read line from stdin to produce
  char buf[512];
  while (run && fgets(buf, sizeof(buf), stdin)) {
    size_t len = strlen(buf);
    if (len > 0 && buf[len - 1] == '\n')  // FIXME: is always len>0?
      buf[--len] = '\0';

    if (len > 0) {
      ++num_message;
      ++num_message_left;
      error::Print("[INFO] Produce %zd bytes...\n", len);
      bool success = false;
      while (!(success = producer.produce(topic, buf, len))) {
        // check producer() error reason
        error_code = rd_kafka_last_error();
        error::Print("[INFO] Failed to produce to topic %s: %s\n", topic_name,
                     rd_kafka_err2str(error_code));

        // queue is full, wait for 1000ms, trigger dr_msg_cb()
        // queue's size is determined by queue.buffering.max.messages
        if (error_code == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
          error::Print("[INFO] Queue is full!\n");
          producer.poll(1000);
        } else {
          break;
        }
      }

      if (success) {
        error::Print("[INFO] Enqueued message (%zd bytes) for topic %s\n", len,
                     topic_name);
      }
    }

    producer.poll(0);  // trigger dr_msg_cb()
  }

  // wait for message delivery success
  while (num_message_left > 0) {
    error::Print("[INFO] Flushing final messages...\n");
    producer.flush(1 * 1000);
  }
  error::Print("[INFO] DONE! %zd messages committed!\n", num_message);
  return 0;
}

static void dr_msg_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage,
                      void* opaque) {
  if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    error::Print("[INFO] Message delivered (%zd bytes, partition %d)]\n",
                 rkmessage->len, rkmessage->partition);
  } else {
    error::Print("[ERROR] Message delivery failed: %s\n",
                 rd_kafka_err2str(rkmessage->err));
  }
  --num_message_left;
}
