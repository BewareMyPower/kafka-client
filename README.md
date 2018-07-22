# rdkafka-client
Simple and lightweight Linux C++11 encapsulation on [librdkafka](https://github.com/edenhill/librdkafka)'s C API.
1. Use `std::unique_ptr<T>` to manage pointers like `rd_kafka_t*`;
2. Use a simple config format. (I've tried TOML but found it unnecessary);
3. Use variadic template to implement formatted error handle functions.

# Why not use [cppkafka](https://github.com/mfontanini/cppkafka)
1. Just for C++11 practice
2. Not require boost library.

I've only encapsulated some necessary functions to use, it's easy to extend these header files for your own use.

# How to use
It's a header-only library and there's a sample Makefile, but `ROOT_RDKAFKA` should be modified to *librdkafka*'s root directory.
First open [Makefile](examples/Makefile) and modify `ROOT_RDKAFKA` to your librdkafka install directory.
```
cd examples
make
./producer <topic-name> <config-path>
./consumer <topic-name> <config-path>
```
Log's printed to stderr, so you can use `./consumer <topic-name> <config-path> 2>err` to watch message in terminal
and check for log later.

# NOTE
old version kafka must add these global configure(my kafka version is 0.9.0.1)
see [Broker version compatibility](https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility)
```
api.version.request=false
broker.version.fallback=0.9.0.1
```
I've struggled with this problem for a while, it's really important to known how [Kafka](https://kafka.apache.org/documentation.html) works and read [librdkafka WIKI](https://github.com/edenhill/librdkafka/wiki) first.
