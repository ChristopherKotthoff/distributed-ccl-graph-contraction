# Use an official Python runtime as a parent image
FROM orwel84/ubuntu-16-mpi

# Set the working directory in the container to /app
WORKDIR /app

# Update the package list for the local repository
RUN apt-get update

# Install build-essential (which includes 'make')
RUN apt-get install -y build-essential
RUN apt-get install -y gdb
