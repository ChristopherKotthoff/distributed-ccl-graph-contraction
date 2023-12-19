CC = mpic++
CFLAGS  = -O0 -g -Wall -Wno-unused-variable -pedantic -std=c++11 -I/usr/include/hdf5/serial
DEBUGFLAGS = -v -fsanitize=address
MPI_RANKS ?= 2  # Default to 2 ranks
LATE_FLAGS =  -lhdf5_cpp -lhdf5

all:
	$(CC) $(CFLAGS) -o main main.cpp $(LATE_FLAGS)

run:
	$(CC) $(CFLAGS) -o main main.cpp $(LATE_FLAGS)
	mpiexec -n $(MPI_RANKS) --oversubscribe --allow-run-as-root main

debug:
	$(CC) $(CFLAGS) $(DEBUGFLAGS) -o main main.cpp $(LATE_FLAGS)
	mpiexec -n $(MPI_RANKS) --oversubscribe --allow-run-as-root main

preprocessed:
	$(CC) -E -C -o preprocessed.cpp main.cpp $(CFLAGS) $(LATE_FLAGS)

runpreprocessed:
	$(CC) $(CFLAGS) $(DEBUGFLAGS) -o main preprocessed.cpp $(LATE_FLAGS)
	mpiexec -n $(MPI_RANKS) --oversubscribe --allow-run-as-root main

clean:
	rm -f main
