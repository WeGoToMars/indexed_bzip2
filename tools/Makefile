CPP=clang++
CFLAGS=-I ../indexed_bzip2 -std=c++17 -Wall -Wextra -Wshadow -Wsuggest-override -g
CFLAGS=-I ../indexed_bzip2 -std=c++17 -Wall -Wextra -Wshadow -Wsuggest-override -O3 -DNDEBUG
LFLAGS=-pthread

all: bzcat blockfinder pbzcat

bzcatpipe: bzcatpipe.o
	$(CPP) $^ -o $@

bzcatpipe.o: bzcatpipe.cpp ../indexed_bzip2/bzip2.hpp ../indexed_bzip2/BitReader.hpp ../indexed_bzip2/FileReader.hpp
	$(CPP) $(CFLAGS) -c $< -o $@

bzcat: bzcat.o
	$(CPP) $(LFLAGS) $^ -o $@

bzcat.o: bzcat.cpp ../indexed_bzip2/bzip2.hpp ../indexed_bzip2/BZ2Reader.hpp ../indexed_bzip2/BitReader.hpp ../indexed_bzip2/FileReader.hpp
	$(CPP) $(CFLAGS) -c $< -o $@

pbzcat: pbzcat.o
	$(CPP) $(LFLAGS) $^ -o $@

pbzcat.o: pbzcat.cpp ../indexed_bzip2/bzip2.hpp ../indexed_bzip2/BlockDatabase.hpp ../indexed_bzip2/ParallelBZ2Reader.hpp ../indexed_bzip2/BitReader.hpp ../indexed_bzip2/ThreadSafeQueue.hpp ../indexed_bzip2/ThreadPool.hpp ../indexed_bzip2/FileReader.hpp ../indexed_bzip2/BitStringFinder.hpp
	$(CPP) $(CFLAGS) -c $< -o $@

blockfinder: blockfinder.o
	$(CPP) $(LFLAGS) $^ -o $@

blockfinder.o: blockfinder.cpp ../indexed_bzip2/bzip2.hpp ../indexed_bzip2/BitReader.hpp ../indexed_bzip2/FileReader.hpp
	$(CPP) $(CFLAGS) -c $< -o $@
