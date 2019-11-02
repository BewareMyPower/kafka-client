#include "kafka_client/config.h"

#include <iostream>
using namespace std;

int main(int argc, char* argv[]) {
  kafka_client::GlobalConfig config;

  auto test_get = [&config](const char* key) {
    auto value = config.Get(key);
    if (!value.empty()) {
      cout << "[OK] " << key << "=" << value << endl;
    } else {
      cerr << "[FAILED] " << config.error() << endl;
    }
  };

  test_get("bootstrap.servers");
  test_get("api.version.request");

  auto test_put = [&config](const char* key, const char* value) {
    if (config.Put(key, value)) {
      cout << "[OK] Put " << key << "=" << value << endl;
    } else {
      cerr << "[FAILED] " << config.error() << endl;
    }
  };

  test_put("bootstrap.servers", "localhost:9092");
  test_put("WTF???", "WTF???");
  test_put("api.version.request", "false");
  test_put("api.version.request", "NOT_TRUE_OR_FALSE");

  test_get("api.version.request");

  return 0;
}
