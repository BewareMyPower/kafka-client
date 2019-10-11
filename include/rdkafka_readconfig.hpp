// rdkafka_readconfig.hpp: read custom config file example
// ---------------------config starts-------------------------
// [topic]
// key1=value1  # comment1
// key2=value2
//
// # comment2
// [global]
// key1=value1
// key2=value2
// key3=value3
// ---------------------config ends---------------------------
// NOTE: the header must be [topic] or [global], other lines must
//       be "key=value" format or empty line.
#pragma once

#include <ctype.h>

#include <algorithm>
#include <fstream>
#include <string>
#include <unordered_set>
#include <utility>

#include "rdkafka_classes.hpp"
#include "rdkafka_error.hpp"

namespace rdkafka {

// User shouldn't call it
inline std::string __strip(const std::string& s) {
  // TODO: use std::reverse_iterator and std::find_if()
  size_t pos_begin = 0;
  while (pos_begin < s.size() && isblank(s[pos_begin])) ++pos_begin;
  if (pos_begin == s.size() || s[pos_begin] == '#') return "";

  size_t pos_end = s.size();
  while (pos_end > pos_begin && isblank(s[pos_end - 1])) --pos_end;
  // s[pos_begin, pos_end)

  size_t pos_comment = s.rfind('#', pos_end - 1);
  if (pos_comment != std::string::npos) {
    pos_end = pos_comment;  // NOTE: pos_comment >= pos_begin
  }

  return s.substr(pos_begin, pos_end - pos_begin);
}

inline std::pair<GlobalConf, TopicConf> readConfig(
    const std::string& filename) {
  std::ifstream fin(filename);
  error::ExitIf(!fin, "open file \"%s\" failed!\n", filename.data());

  std::string line;
  int num_line = 0;

  std::pair<GlobalConf, TopicConf> configs;
  enum { GLOBAL, TOPIC, NONE } type = NONE;

  while (std::getline(fin, line)) {
    std::string old_line = line;
    line = __strip(line);
    ++num_line;

    if (line.empty()) continue;

    // read header "[global]" or "[topic]"
    if (line[0] == '[') {
      if (line.back() != ']' || line.length() < 2)
        error::Exit("[file: %s] line %d (\"%s\") is invalid header\n",
                    filename.data(), num_line, old_line.data());

      auto header = line.substr(1, line.length() - 2);
      if (header == "topic")
        type = TOPIC;
      else if (header == "global")
        type = GLOBAL;
      else
        error::Exit("header \"%s\" is not \"topic\" or \"global\"\n",
                    header.data());

      continue;
    }

    if (type == NONE) {
      error::Exit(
          "[file: %s] line %d doesn't have a header like "
          "\"[topic]\" or \"[global]\"\n",
          filename.data(), num_line);
    }

    size_t pos = line.find('=');
    if (pos == std::string::npos) {
      error::Exit("[file: %s] line %d (\"%s\"): can't find '='\n",
                  filename.data(), num_line, old_line.data());
    }

    line[pos] = '\0';
    error::Print("--CONFIG [%s=%s]\n", line.data(), line.data() + pos + 1);
    if (type == GLOBAL)
      configs.first.put(line.data(), line.data() + pos + 1);
    else if (type == TOPIC)
      configs.second.put(line.data(), line.data() + pos + 1);
    else
      error::Exit("Unknown error in line %d\n", num_line);
  }

  return configs;
}

}  // namespace rdkafka
