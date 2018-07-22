// rdkafka_error.hpp: librdkafka error handler
#pragma once

#include "rdkafka.h"

#include <stdio.h>
#include <stdlib.h>

#include <utility>

namespace rdkafka {

using ErrorCode = rd_kafka_resp_err_t;

namespace error {

template <typename ...Args>
inline void Print(const char* format, Args... args) {
    fprintf(stderr, format, args...);
}

inline void Print(const char* msg) {
    fputs(msg, stderr);
}

template <typename ...Args>
inline void Exit(Args... args) {
    Print(std::forward<Args>(args)...);
    exit(1);
}

template <typename ...Args>
inline void ExitIf(bool condition, Args... args) {
    if (condition)
        Exit(std::forward<Args>(args)...);
}

template <typename T>
inline void checkHandleNotNull(T* handle, const char* msg, const char* errstr) {
    ExitIf(!handle, "%s failed: %s\n", handle, msg, errstr);
}

template <typename T>
inline void checkHandleNotNull(T* handle, const char* msg) {
    ExitIf(!handle, "%s failed\n", handle, msg);
}

inline void checkRespError(rd_kafka_resp_err_t error_code, const char* msg) {
    ExitIf(error_code != RD_KAFKA_RESP_ERR_NO_ERROR,
           "%s failed: %s\n", msg, rd_kafka_err2str(error_code));
}

inline void checkUnixErrorRetval(int ret, const char* msg) {
    ExitIf(ret == -1, "%s failed: %s\n", msg, rd_kafka_err2str(rd_kafka_last_error()));
}

}  // namespace rdkafka::error

}  // namespace rdkafka
