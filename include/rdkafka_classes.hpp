// rdkafka_classes.hpp: librdkafka struct wrappers
#pragma once

#include "rdkafka.h"

#include <string.h>

#include <memory>
#include <string>
#include <vector>

#include "rdkafka_util.hpp"
#include "rdkafka_error.hpp"

namespace rdkafka {

class TopicConf;
class GlobalConf;
class Topic;
class KafkaBase;
class Consumer;
class Producer;
class Message;

template <typename T>
class PointerHolder {
public:
    using element_type = T;
    using pointer = T*;
    using deleter_type = void (*)(pointer);

    PointerHolder(pointer ptr, deleter_type deleter) noexcept : ptr_(ptr, deleter) {}

    // automatic following member:
    // 1. deleted copy constructor/assignment operator
    // 2. default move constructor/assignment operator
    // 3. default destructor

    pointer get() const noexcept { return ptr_.get(); }
    pointer release() noexcept { return ptr_.release(); }

    bool isNull() const noexcept { return ptr_ == nullptr; }

private:
    std::unique_ptr<T, deleter_type> ptr_;
};

template <typename T>
class Conf : public PointerHolder<T> {
public:
    using Base = PointerHolder<T>;
    using typename Base::pointer;
    using typename Base::element_type;
    using typename Base::deleter_type;
    using Base::Base;

    Conf(pointer ptr, deleter_type deleter) : Base(ptr, deleter) {}

    void put(const char* name, const char* value) const noexcept {
        util::putNameValue(this->get(), name, value);
    }
};

class TopicConf : public Conf<rd_kafka_topic_conf_t> {
public:
    using Base = Conf<rd_kafka_topic_conf_t>;
    TopicConf() noexcept : Base(rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy) {}
};

class GlobalConf : public Conf<rd_kafka_conf_t> {
public:
    using Base = Conf<rd_kafka_conf_t>;
    using RebalanceCallBack = void(*)(rd_kafka_t*, rd_kafka_resp_err_t,
            rd_kafka_topic_partition_list_t*, void*);
    using DeliveryReportCallback = void(*)(rd_kafka_t*,
            const rd_kafka_message_t*, void*);

    GlobalConf() noexcept : Base(rd_kafka_conf_new(), rd_kafka_conf_destroy) {}

    void setDefaultTopicConf(TopicConf&& topicconf) const noexcept {
        // take rd_kafka_topic_conf_t's ownership
        rd_kafka_conf_set_default_topic_conf(get(), topicconf.release());
    }

    void setRebalanceCallback(RebalanceCallBack callback) const noexcept {
        rd_kafka_conf_set_rebalance_cb(get(), callback);
    }

    void setDeliveryReportCallback(
            DeliveryReportCallback callback) const noexcept {
        rd_kafka_conf_set_dr_msg_cb(get(), callback);
    }
};

class KafkaBase : public PointerHolder<rd_kafka_t> {
public:
    using Base = PointerHolder<rd_kafka_t>;

    int poll(int timeout_ms) const noexcept {
        return rd_kafka_poll(get(), timeout_ms);
    }

    bool flush(int timeout_ms) const noexcept {
        return rd_kafka_flush(get(), timeout_ms) != RD_KAFKA_RESP_ERR__TIMED_OUT;
    }

    void dump(FILE* fp = stderr) const {
        rd_kafka_dump(fp, get());
    }

    const char* getName() const noexcept {
        return rd_kafka_name(get());
    }

protected:
    // store error in errstr
    template <size_t N>
    KafkaBase(rd_kafka_type_t type, GlobalConf conf, char (&errstr)[N]) noexcept
        : Base(rd_kafka_new(type, conf.release(), errstr, N), rd_kafka_destroy) {}
};

class Topic : public PointerHolder<rd_kafka_topic_t> {
public:
    using Base = PointerHolder<rd_kafka_topic_t>;

    Topic(rd_kafka_t* rk, const char* name) noexcept
        : Base(rd_kafka_topic_new(rk, name, nullptr), rd_kafka_topic_destroy) {}

    Topic(rd_kafka_t* rk, const char* name, TopicConf conf) noexcept
        : Base(rd_kafka_topic_new(rk, name, conf.release()), rd_kafka_topic_destroy) {}
};

class Producer final : public KafkaBase {
public:
    template <size_t N>
    Producer(GlobalConf conf, char (&errstr)[N]) noexcept
        : KafkaBase(RD_KAFKA_PRODUCER, std::move(conf), errstr) {}

    bool produce(const Topic& topic, char* payload, size_t len,
                 const char* key = nullptr, size_t keylen = 0,
                 void* msg_opaque = nullptr) const noexcept {
        return rd_kafka_produce(topic.get(), partition_, msgflags_,
                                static_cast<void*>(payload), len,
                                static_cast<const void*>(key), keylen,
                                msg_opaque) == 0;
    }

    void setPartition(int32_t partition) noexcept { partition_ = partition; }

    void setMsgflags(int msgflags) noexcept { msgflags_ = msgflags; }

private:
    int32_t partition_ = RD_KAFKA_PARTITION_UA;
    int msgflags_ = RD_KAFKA_MSG_F_COPY;
};

class Message final : public PointerHolder<rd_kafka_message_t> {
    friend class Consumer;
public:
    using Base = PointerHolder<rd_kafka_message_t>;
    using typename Base::pointer;
    using typename Base::element_type;

//    pointer operator->() const noexcept { return get(); }

    const char* getPayload() const noexcept {
        return const_cast<const char*>(static_cast<char*>(get()->payload));
    }

    size_t getPayloadLen() const noexcept { return get()->len; }

    const char* getKey() const noexcept {
        return const_cast<const char*>(static_cast<char*>(get()->key));
    }

    size_t getKeyLen() const noexcept { return get()->key_len; }

    bool hasError() const noexcept { return get()->err != RD_KAFKA_RESP_ERR_NO_ERROR; }

    const char* getErrorStr() const noexcept {
        return rd_kafka_message_errstr(get());
    }
    
    int32_t getPartition() const noexcept { return get()->partition; }

    int64_t getOffset() const noexcept { return get()->offset; }

    const char* getTopicName() const noexcept {
        auto p = get();
        if (p->rkt) {
            return rd_kafka_topic_name(p->rkt);
        } else {
            return "unknown topic";
        }
    }

    bool isTopicInvalid() const noexcept {
        return get()->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
    }

    bool isPartitionInvalid() const noexcept {
        return get()->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
    }

private:
    Message(pointer ptr) noexcept : Base(ptr, rd_kafka_message_destroy) {}
};

class Consumer final : public KafkaBase {
public:
    template <size_t N>
    Consumer(GlobalConf conf, char (&errstr)[N]) noexcept
        : KafkaBase(RD_KAFKA_CONSUMER, std::move(conf), errstr) {
        auto error_code = rd_kafka_poll_set_consumer(get());
        if (error_code != RD_KAFKA_RESP_ERR_NO_ERROR) {
            const char* errmsg = rd_kafka_err2str(rd_kafka_last_error());
            strncpy(errstr, errmsg, N);
        }
    }

    ErrorCode subscribe(const std::vector<std::string>& topics) const noexcept {
        auto topic_list = rd_kafka_topic_partition_list_new(static_cast<int>(topics.size()));
        if (!topic_list)
            return rd_kafka_last_error();

        for (auto const& topic : topics) {
            rd_kafka_topic_partition_list_add(topic_list,
                    topic.data(), RD_KAFKA_PARTITION_UA);
        }

        auto res = rd_kafka_subscribe(get(), topic_list);
        rd_kafka_topic_partition_list_destroy(topic_list);
        return res;
    }

    Message consume(int timeout_ms) const noexcept {
        return rd_kafka_consumer_poll(get(), timeout_ms);
    }

    ErrorCode waitUntilRebalanceRevoke() const noexcept {
        return rd_kafka_consumer_close(get());
    }
};

}  // namespace rdkafka
