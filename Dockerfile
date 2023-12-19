# Use an official Python runtime as a parent image
FROM orwel84/ubuntu-16-mpi

# Set the working directory in the container to /app
WORKDIR /app

# Update the package list for the local repository
RUN apt-get update

# Install build-essential (which includes 'make')
RUN apt-get install -y build-essential

# Install gdb
RUN apt-get install -y gdb

# Install Python 3 and pip
RUN apt-get install -y python3 python3-pip

Run ln -s -f /usr/bin/python3 /usr/bin/python
Run ln -s -f /usr/bin/pip3 /usr/bin/pip

# Optionally install additional Python packages
RUN pip3 install "numpy<1.19"
RUN pip3 install "h5py<3.0"

# Install HDF5 development libraries and tools
RUN apt-get install -y libhdf5-dev

ENV HDF5_DIR /usr/lib/x86_64-linux-gnu/hdf5/serial
ENV LD_LIBRARY_PATH $HDF5_DIR/lib:$LD_LIBRARY_PATH
ENV CPATH $HDF5_DIR/include:$CPATH
ENV LIBRARY_PATH $HDF5_DIR/lib:$LIBRARY_PATH
