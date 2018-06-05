#Constants for flags and commands:
CXX = g++
CXXFLAGS = -c -Wall -std=c++11
FILES = Barrier.cpp Barrier.h Makefile MapReduceFramework.cpp README

all: MapReduceFrameworkLib

#Library for libMapReduceFramework.a
MapReduceFrameworkLib: MapReduceFramework.o Barrier.o
		ar -rcs libMapReduceFramework.a MapReduceFramework.o Barrier.o

#Takes care for MapReduceFramework.o
MapReduceFramework.o: MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h Barrier.o
		$(CXX) $(CXXFLAGS) MapReduceFramework.cpp -o MapReduceFramework.o

#Takes care for Barrier.o
Barrier.o: Barrier.cpp Barrier.h
		$(CXX) $(CXXFLAGS) Barrier.cpp -o Barrier.o


# Makes the tar file:
tar:
		tar -cvf ex3.tar $(FILES)

# cleaning:
clean:
		rm -f *.o libMapReduceFramework.a ex3.tar
		rm -f *.out



