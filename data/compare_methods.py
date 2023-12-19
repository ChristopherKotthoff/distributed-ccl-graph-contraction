import h5py
import time

def measure_loading_time(file_name, x, y):
    # Measure time for loading from .txt file
    start_time_txt = time.time()
    with open(file_name + ".txt", 'r') as file:
        lines_txt = [line.strip() for line in list(file)[x:y]]
    end_time_txt = time.time()
    txt_loading_time = end_time_txt - start_time_txt

    # Measure time for loading from .h5 file
    start_time_h5 = time.time()
    with h5py.File(file_name + ".h5", 'r') as h5file:
        lookup_table = h5file['lookup']
        data = h5file['data']

        # Get the indices from the lookup table
        start_index, _ = lookup_table[x]
        _, end_index = lookup_table[y - 1]  # y - 1 because ranges are exclusive at the end

        # Read the corresponding range from the data chunk
        lines_h5 = data[start_index:end_index + 1]
    end_time_h5 = time.time()
    h5_loading_time = end_time_h5 - start_time_h5

    return txt_loading_time, h5_loading_time

# Example usage:
file_base_name = "coauth-DBLP-full-proj-graph-LIST_REMAP"  # Replace with your file base name
x, y = 0, 1800000  # Define the range of lines you want to read
txt_time, h5_time = measure_loading_time(file_base_name, x, y)
print("TXT Loading Time:", txt_time)
print("H5 Loading Time:", h5_time)

