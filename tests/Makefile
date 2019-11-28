ROOT_PROJECT = ..
ROOT_RDKAFKA = ..

CXX = g++
CXXFLAGS = -std=c++11 -g -I .. -I $(ROOT_RDKAFKA)/include/librdkafka \
		   -I $(ROOT_PROJECT)/include \
		   -Wall -Wsign-compare -Wfloat-equal -Wpointer-arith -Wcast-align
LDFLAGS = -L $(ROOT_RDKAFKA)/lib
LDLIBS = -lrdkafka -lz -lpthread -lrt -Wl,-rpath=$(ROOT_RDKAFKA)/lib

SOURCES = error_message_test.cc config_test.cc
TARGETS = $(SOURCES:.cc=.out)

all: $(TARGETS)
	@echo "Build targets: $(TARGETS)"

%.out: %.cc
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(LDLIBS) $< -o $@

clean:
	rm -rf $(TARGETS)
