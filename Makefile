CC=g++
CXX=g++
RANLIB=ranlib

LIBSRC=Barrier.h Barrier.cpp MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h 
LIBOBJ= MapReduceFramework.o Barrier.o
TARSRCS = Barrier.h Barrier.cpp MapReduceFramework.cpp Makefile README

INCS=-I.
CFLAGS = -Wall -std=c++11 -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -g $(INCS)

OSMLIB = libMapReduceFramework.a
TARGETS = $(OSMLIB)

TAR=tar
VAL=valgrind
TARFLAGS=-cvf
VALGFLAGS=leak-check=full
VALGFLAGS1=show-possibly-lost=yes
VALGFLAGS2=show-reachable=yes
TARNAME=ex3.tar

.PHONY: all, clean, tar

all: $(TARGETS)

$(TARGETS): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

clean:
	$(RM) $(LIBOBJ) $(OSMLIB) *~ *core


depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
