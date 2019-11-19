ROOT_RDKAFKA = ..

CXX = g++
CXXFLAGS = -std=c++11 -g -I .. -I $(ROOT_RDKAFKA)/include/librdkafka \
		   -Wall -Wsign-compare -Wfloat-equal -Wpointer-arith -Wcast-align
LDFLAGS = -L $(ROOT_RDKAFKA)/lib
LDLIBS = -lrdkafka -lz -lpthread -lrt -Wl,-rpath=$(ROOT_RDKAFKA)/lib

PROGS = producer consumer group_metadata offset_manager

all: $(PROGS)

%: %.cc
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(LDLIBS) $< -o $@

clean:
	rm -rf $(PROGS)
