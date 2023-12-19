import h5py
import numpy as np

def create_hdf5_with_lookup(input_file, output_file):
    data = []  # Store the flattened data with -1 as separators
    lookup = []  # Store start and end indices for each line

    current_index = 0
    with open(input_file, 'r') as file:
        for line in file:
            numbers = list(map(int, line.strip().split()))
            data.extend(numbers)
            data.append(-1)  # Add -1 as a separator
            lookup.append((current_index, current_index + len(numbers)))
            current_index += len(numbers) + 1

    # Convert to numpy arrays
    data_array = np.array(data, dtype='int32')
    lookup_array = np.array(lookup, dtype='int32')

    # Create HDF5 file
    with h5py.File(output_file, 'w') as h5file:
        h5file.create_dataset('data', data=data_array)
        h5file.create_dataset('lookup', data=lookup_array)
        h5file.create_dataset('vertices', data=lookup_array.shape[0])

# Example usage
#create_hdf5_with_lookup('coauth-DBLP-full-proj-graph-LIST_REMAP.txt', 'coauth-DBLP-full-proj-graph-LIST_REMAP.h5')


def read_lines_from_hdf5(file_name, start_line, end_line):
    with h5py.File(file_name, 'r') as h5file:
        vertices = h5file['vertices'][()]
        print(vertices)
        lookup = h5file['lookup']
        data_chunk = h5file['data']

        start_index = lookup[start_line, 0]
        end_index = lookup[end_line, 1]

        # Retrieve and process the data chunk
        chunk = data_chunk[start_index:end_index + 1]
        lines = []
        line = []
        for number in chunk:
            if number == -1:
                lines.append(line)
                line = []
            else:
                line.append(number)
        return lines

# Usage
lines = read_lines_from_hdf5('coauth-DBLP-full-proj-graph-LIST_REMAP.h5', 9, 18)
print(lines)

