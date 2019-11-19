#ifndef KAFKA_CLIENT_ERROR_MESSAGE_H
#define KAFKA_CLIENT_ERROR_MESSAGE_H

#include <string>

namespace kafka_client {

class ErrorMessage {
 public:
  const char* data() const noexcept { return data_.data(); }

  ErrorMessage& operator=(const char* s) {
    data_ = s;
    return *this;
  }

  ErrorMessage& operator=(const std::string& s) {
    return ((*this) = (s.c_str()));
  }

  template <typename... Args>
  void Format(const char* format, Args&&... args);

  template <typename... Args>
  void AppendFormat(const char* format, Args&&... args);

 protected:
  std::string data_;
};

}  // namespace kafka_client

#include <assert.h>
#include <stdio.h>

namespace kafka_client {

template <typename... Args>
inline void ErrorMessage::Format(const char* format, Args&&... args) {
  int n = snprintf(nullptr, 0, format, std::forward<Args>(args)...);
  assert(n > 0);

  data_.resize(n);
  snprintf(&data_[0], n + 1, format, std::forward<Args>(args)...);
}

template <typename... Args>
inline void ErrorMessage::AppendFormat(const char* format, Args&&... args) {
  int n = snprintf(nullptr, 0, format, std::forward<Args>(args)...);
  assert(n > 0);

  auto old_size = data_.size();
  data_.resize(old_size + n);

  snprintf(&data_[old_size], n + 1, format, std::forward<Args>(args)...);
}

}  // namespace kafka_client

#endif  // KAFKA_CLIENT_ERROR_MESSAGE_H
