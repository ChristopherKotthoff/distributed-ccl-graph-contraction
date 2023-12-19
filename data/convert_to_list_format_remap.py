def convert_to_adjacency_list(input_file, output_file):
    # Initialize an empty dictionary for the adjacency list
    adjacency_list = {}
    remap = {}
    # Read the file and populate the adjacency list
    with open(input_file, 'r') as file:
        for line in file:
            from_node, to_node, _ = map(int, line.split())
            if from_node not in adjacency_list:
                adjacency_list[from_node] = []
            if to_node not in adjacency_list:
                adjacency_list[to_node] = []
            adjacency_list[from_node].append(to_node)
            adjacency_list[to_node].append(from_node)

    # Sort the adjacency list by node id and connected node ids
    sorted_adjacency_list = {k: sorted(v) for k, v in sorted(adjacency_list.items())}

    current_node_id = 0
    
    for node, connected_nodes in sorted_adjacency_list.items():
        if len(connected_nodes) == 0:
            continue
        else:
            remap[node] = current_node_id
            current_node_id += 1

    adjacency_list_new = {}
    for node, connected_nodes in sorted_adjacency_list.items():
        if len(connected_nodes) == 0:
            continue
        else:
            temp = []
            for i in connected_nodes:
                temp.append(remap[i])
            adjacency_list_new[remap[node]] = temp

    sorted_adjacency_list_new = {k: sorted(v) for k, v in sorted(adjacency_list_new.items())}


    # Write the sorted adjacency list to the output file
    with open(output_file, 'w') as file:
        for node, connected_nodes in sorted_adjacency_list_new.items():
            file.write("%s %s\n" % (node, ' '.join(map(str, connected_nodes))))

# Example usage
convert_to_adjacency_list('coauth-DBLP-full-proj-graph.txt', 'coauth-DBLP-full-proj-graph-LIST_REMAP.txt')
