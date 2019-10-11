#ifndef TIMESTAMP_H
#define TIMESTAMP_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>

namespace helper {

const char* timestampToString(int64_t timestamp_ms) {
  if (timestamp_ms <= 0) return "<Negative Timestamp>";
  static char buf[50];

  auto secs = static_cast<time_t>(timestamp_ms / 1000);
  auto n = strftime(buf, sizeof(buf), "%F %T", localtime(&secs));
  if (n >= sizeof(buf) - 4) {
    fprintf(stderr, "timestampToString: inner bufsize(%zu) is too small\n",
            sizeof(buf));
    return buf;
  }

  snprintf(buf + n, sizeof(buf) - n, ".%03d",
           static_cast<int>(timestamp_ms % 1000));
  return buf;
}

}  // namespace helper

#endif  // TIMESTAMP_H
