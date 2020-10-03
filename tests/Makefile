CPP=g++
CFLAGS=-I ../indexed_bzip2 -std=c++14 -Wall -Wextra -Wshadow -Wsuggest-override -O3 -g
LFLAGS=-pthread

all: testBitStringFinder

testBitStringFinder: testBitStringFinder.o
	$(CPP) $(LFLAGS) $^ -o $@

testBitStringFinder.o: testBitStringFinder.cpp ../indexed_bzip2/BitStringFinder.hpp
	$(CPP) $(CFLAGS) -c $< -o $@
