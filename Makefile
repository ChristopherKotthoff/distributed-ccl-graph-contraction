CC = mpic++
CFLAGS  = -O0 -g -Wall -Wno-unused-variable -pedantic -std=c++11
DEBUGFLAGS = -v -fsanitize=address
MPI_RANKS ?= 2  # Default to 2 ranks

all:
	$(CC) $(CFLAGS) -o main main.cpp

run:
	$(CC) $(CFLAGS) -o main main.cpp
	mpiexec -n $(MPI_RANKS) --oversubscribe --allow-run-as-root main

debug:
	$(CC) $(CFLAGS) $(DEBUGFLAGS) -o main main.cpp
	mpiexec -n $(MPI_RANKS) --oversubscribe --allow-run-as-root main

preprocessed:
	$(CC) -E -C -o preprocessed.cpp main.cpp

runpreprocessed:
	$(CC) $(CFLAGS) $(DEBUGFLAGS) -o main preprocessed.cpp
	mpiexec -n $(MPI_RANKS) --oversubscribe --allow-run-as-root main

clean:
	rm -f main
