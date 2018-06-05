CC = g++
STD = -std=gnu++11

MAP_REDUCE_OBJS = MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h
test_2.cpp Barrier.cpp Barrier.h

TAROBJECTS = uthreads.cpp uthreads.h Makefile README

CFLAGS = -Wextra -Wvla -Wall -Wno-unused-parameter

all: map_reduce

map_reduce: $(MAP_REDUCE_OBJS)
	${CC} $(STD) ${CFLAGS} -c MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h
                              test_2.cpp Barrier.cpp Barrier.h -o mapreduce.o
	ar rcs libuthreads.a mapreduce.o

tar:
	tar cvf ex3.tar ${TAROBJECTS}

clean:
	rm -f ex2.tar mapreduce.o mapreduce.a

.PHONY: all uthreads tar clean
