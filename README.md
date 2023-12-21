# How to run

Either install the dependencies yourself (OpenMPI and HDF5) or use the provided Dockerfile that will provide you with a container with all the dependencies installed.

Then you can just call "make run" to compile and run the program.

# Data

To prepare data for this program, it needs to be in a specific format.
It needs to be a file of 3 integers per row, where the first two are "source" and "destination" of an edge and the third is irrelevant.
Then convert the this edge-set into an edge-list using the provided script. Maybe change up the file path in the script to the data you want to convert.

```bash
cd data
python convert_to_list_format.py
```

Then you can convert it into an HDF5 file using the provided script. Maybe change up the file path in the script to the data you want to convert.

```bash
python convert_to_hdf5.py
```

The file path to the HDF5 file is hardcoded in the C++ program, so you need to change it to the file you want to use.
Place the data set "coauth-DBLP-full-proj-graph.txt" in the data folder and run the two scripts in case you don't want to change the hardcoded paths.

# Do all of this in docker container

Best, use VSCode with the Remote-Containers extension. It will automatically build the container and mount the project folder into the container. Press ctrl+shift+p and choose option "Dev Container: Reopen in Container".

You can also call "make run MPI_RANKS=<some_number>" to run the program with a different number of MPI ranks. The default is 2. It needs to be a power of 2.
