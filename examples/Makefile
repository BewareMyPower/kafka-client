ROOT_RDKAFKA = ~/local/librdkafka

ROOT_MYLIB = ..
CC = g++
FLAGS = -std=c++11 -g -Wall -Wsign-compare -Wfloat-equal -Wpointer-arith -Wcast-align
INCLUDE = $(ROOT_RDKAFKA)/include/librdkafka
LIB = $(ROOT_RDKAFKA)/lib
LINK = -lrdkafka -lz -lpthread -lrt

PROGS = producer consumer

all: $(PROGS)

%: %.cc
	$(CC) $(FLAGS) -I $(INCLUDE) -I $(ROOT_MYLIB) -L $(LIB) $< $(LINK) -o $@

clean:
	rm -rf $(PROGS)
