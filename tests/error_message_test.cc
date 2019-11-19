#include "kafka_client/error_message.h"

#include <iostream>
using namespace std;

int main(int argc, char *argv[]) {
  kafka_client::ErrorMessage message;

  message = "hello world";
  cout << message.data() << endl;

  message.Format("%s: [%d, %f, %c]", "list", 1, 3.14, 'a');
  cout << message.data() << endl;

  message.AppendFormat(": %p", &message);
  cout << message.data() << endl;

  return 0;
}
