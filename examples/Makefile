ROOT_RDKAFKA = ~/local/librdkafka

CXX = g++
CXXFLAGS = -std=c++11 -g -I .. -I $(ROOT_RDKAFKA)/include/librdkafka \
		   -Wall -Wsign-compare -Wfloat-equal -Wpointer-arith -Wcast-align
LDFLAGS = -L $(ROOT_RDKAFKA)/lib
LDLIBS = -lrdkafka -lz -lpthread -lrt

PROGS = producer consumer group_metadata

all: $(PROGS)

%: %.cc
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(LDLIBS) $< -o $@

clean:
	rm -rf $(PROGS)
