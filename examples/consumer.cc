// consumer.cc
#include <signal.h>
#include <unistd.h>
#include "rdkafka.hpp"
using namespace rdkafka;

static bool run = true;
static void rebalance_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t* partitions,
                         void* opaque);

void msg_consume(const Message& message);

int main(int argc, char* argv[]) {
    // command line config
    if (argc > 1 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0))
        error::Exit("Usage: %s [topic-name] [config-path]\n", argv[0]);
    const char* topic_name = (argc > 1) ? argv[1] : "xyz-topic";
    std::string configpath = (argc > 2) ? argv[2] : "config/consumer.conf";

    // file config
    error::Print("[INFO] Read configuration from %s\n", configpath.data());
    auto configs = readConfig(configpath);
    configs.first.setDefaultTopicConf(std::move(configs.second));
    configs.first.setRebalanceCallback(rebalance_cb);

    // shared error message
    char errstr[512];
    ErrorCode error_code;

    // create consumer from global config
    Consumer consumer(std::move(configs.first), errstr);
    if (consumer.isNull())
        error::Exit("[INFO] Create consumer failed: %s\n", errstr);
    error::Print("[INFO] Create consumer: %s\n", consumer.getName());

    error_code = consumer.subscribe({topic_name});
    error::checkRespError(error_code, "[INFO] Consumer subscribe");
    error::Print("[INFO] Waiting for group balance...\n");

    signal(SIGINT, [](int) {
            if (!run)
                exit(1);  // already break the loop, force to exit
            run = false;  // break the loop
        });

    // loop: consume message
    while (run) {
        auto rkmessage = consumer.consume(1000);
        if (!rkmessage.isNull()) {
            msg_consume(rkmessage);
        }
    }

    error_code = consumer.waitUntilRebalanceRevoke();
    if (error_code != RD_KAFKA_RESP_ERR_NO_ERROR) {
        error::Print("[INFO] Failed to close consumer: %s\n", rd_kafka_err2str(error_code));
    } else {
        error::Print("[INFO] Consumer closed\n");
    }
    
    // clean, wait for consumer to destory
    rd_kafka_destroy(consumer.release());
    int cnt = 5;
    while (cnt-- > 0 && rd_kafka_wait_destroyed(1000) == -1)
        error::Print("[INFO] Waiting for librdkafka to decomission\n");
    if (run <= 0)  // clean failed
        consumer.dump();
    return 0;
}

static void rebalance_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t* partitions,
                         void* opaque) {
    error::Print("[INFO] Group rebalance: ");

    ErrorCode error_code;
    switch (err) {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        error::Print("%d partitions assigned\n", partitions->cnt);

        // You can manual setting the consumer start offset here
        partitions->elems[0].offset = 535;
        partitions->elems[1].offset = 850;
        partitions->elems[2].offset = 582;
        partitions->elems[3].offset = 519;
        partitions->elems[4].offset = 818;

        util::printPartitionList(partitions, stderr);
        
        error_code = rd_kafka_assign(rk, partitions);
        error::checkRespError(error_code, "rd_kafka_assign()");
        break;

    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        error::Print("%d partitions revoked\n", partitions->cnt);
        util::printPartitionList(partitions, stderr);

        rd_kafka_assign(rk, nullptr);
        break;

    default:
        error::Print("failed: %s\n", rd_kafka_err2str(err));
        rd_kafka_assign(rk, nullptr);
        break;
    }
}

void msg_consume(const Message& message) {
    static size_t num_msg = 0;

    if (message.hasError()) {
        error::Print("[ERROR] Consume error for topic \"%s\" [%d] offset %ld: %s\n",
                message.getTopicName(),
                message.getPartition(),
                message.getOffset(),
                message.getErrorStr());

        if (message.isTopicInvalid() || message.isPartitionInvalid()) {
            error::Print("[ERROR] invalid topic or partition!\n");
            run = false;
        }
    } else {
        printf("[Message %zd] \"%s\"[%d]: %lld\n  %.*s\n", ++num_msg,
                message.getTopicName(),
                message.getPartition(),
                static_cast<long long>(message.getOffset()),
                static_cast<int>(message.getPayloadLen()),
                message.getPayload());
    }
}
