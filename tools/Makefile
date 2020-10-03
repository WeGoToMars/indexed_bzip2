CPP=g++
CFLAGS=-I ../indexed_bzip2 -std=c++17 -Wall -Wextra -Wshadow -Wsuggest-override -O3 -DNDEBUG
CFLAGS=-I ../indexed_bzip2 -std=c++17 -Wall -Wextra -Wshadow -Wsuggest-override -g
LFLAGS=-pthread

all: bzcat blockfinder pbzcat

bzcatpipe: bzcatpipe.o
	$(CPP) $^ -o $@

bzcatpipe.o: bzcatpipe.cpp ../indexed_bzip2/bzip2.h ../indexed_bzip2/BitReader.hpp ../indexed_bzip2/FileReader.hpp
	$(CPP) $(CFLAGS) -c $< -o $@

bzcat: bzcat.o
	$(CPP) $(LFLAGS) $^ -o $@

bzcat.o: bzcat.cpp ../indexed_bzip2/bzip2.hpp ../indexed_bzip2/BZ2Reader.hpp ../indexed_bzip2/BitReader.hpp ../indexed_bzip2/FileReader.hpp
	$(CPP) $(CFLAGS) -c $< -o $@

blockfinder: blockfinder.o
	$(CPP) $(LFLAGS) $^ -o $@

blockfinder.o: blockfinder.cpp ../indexed_bzip2/bzip2.h ../indexed_bzip2/BitReader.hpp ../indexed_bzip2/FileReader.hpp
	$(CPP) $(CFLAGS) -c $< -o $@
