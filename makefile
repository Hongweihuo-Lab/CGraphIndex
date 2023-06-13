CXX = g++
CC = gcc
CFLAGS= -std=c++17 -O3 -pedantic -funroll-loops -pthread -march=native
LIB = -lpthread -fopenmp
OBJECT = CSA.o  Gen_Phi.o InArray.o Phi.o loadkit.o savekit.o AdjList.o InputInit.o Graph.o libdivsufsort.a 
OBJINIT = InArray.o loadkit.o savekit.o InputInit.o AdjList.o
TARGET = create_init create_adjlist create_index do_search
all: $(TARGET)

create_init: div create_init.cpp $(OBJINIT)
	$(CXX)  $(LIB) $(CFLAGS) $(OBJINIT) create_init.cpp -o create_init
create_adjlist: div create_adjlist.cpp $(OBJINIT)
	$(CXX)  $(LIB) $(CFLAGS) $(OBJINIT) create_adjlist.cpp -o create_adjlist
create_index: div create_index.cpp $(OBJECT)
	$(CXX)  $(LIB) $(CFLAGS) $(OBJECT) create_index.cpp -o create_index
do_search: div do_search.cpp $(OBJECT)
	$(CXX)  $(LIB) $(CFLAGS) $(OBJECT) do_search.cpp -o do_search


CSA.o: CSA.h CSA.cpp
	$(CXX) -c $(LIB) $(CFLAGS) CSA.cpp 
Gen_Phi.o: ABS_Phi.h Gen_Phi.h Gen_Phi.cpp
	$(CXX) -c $(CFLAGS) Gen_Phi.cpp
InArray.o: InArray.h InArray.cpp
	$(CXX) -c $(CFLAGS) InArray.cpp 
Phi.o: Phi.h Phi.cpp 
	$(CXX) -c $(CFLAGS) Phi.cpp 
loadkit.o: loadkit.h loadkit.cpp
	$(CXX) -c $(CFLAGS) loadkit.cpp 
savekit.o: savekit.h savekit.cpp
	$(CXX) -c $(CFLAGS) savekit.cpp 
Gen_CSA.o: Gen_CSA.cpp
	$(CXX) -c $(CFLAGS) Gen_CSA.cpp
AdjList.o: AdjList.h AdjList.cpp
	$(CXX) -c $(CFLAGS) AdjList.cpp
InputInit.o: InputInit.h InputInit.cpp
	$(CXX) -c $(CFLAGS) InputInit.cpp
Graph.o: Graph.h Graph.cpp
	$(CXX) -c $(CFLAGS) Graph.cpp

libdivsufsort.a:
	cp ./divsufsort/libdivsufsort.a ./
clean:
	-rm $(TARGET) $(OBJECT) ./divsufsort/*.a ./divsufsort/*.o
div:
	make -C ./divsufsort/
