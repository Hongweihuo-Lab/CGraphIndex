SHELL = /bin/sh
CC = g++
CFLAGS = -pedantic -funroll-loops -pthread -std=c++0x -DNDEBUG -O3 -march=native

all: psaa
psaa:interfore.o utils.o
	g++ interfore.o utils.o main.cpp -o main
interfore.o:interfore.cpp interfore.h
	$(CC) -c  $(CFLAGS) $< -o $@ -ldivsufsort -ldivsufsort64 -fopenmp
utils.o:utils.cpp utils.h
	$(CC) -c  $(CFLAGS) $< -o $@
clean:
	/bin/rm -f *.o
nuclear:
	/bin/rm -f psascan *.o
