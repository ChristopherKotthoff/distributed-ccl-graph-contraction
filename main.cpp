//#define _GLIBCXX_DEBUG
#include <vector>
#include <iostream>
#include <mpi.h>
#include <algorithm>
#include <unistd.h>
#include <cassert>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <random>
#include <H5Cpp.h>
#include <stack>

#define DEBUG_CONDITION false
#define RANK_OF_INTEREST 0
#define COUNT_CC true

/// @brief

class CAG
{
public:
    struct Node
    {
        int id;
        bool isForeign;
        std::unordered_set<int> neighbors; // Using unordered_set to avoid duplicates
        bool is_next_to_foreign;
        Node() : id(-1), isForeign(false), is_next_to_foreign(false) {} // Default constructor
        Node(int _id, bool _isForeign) : id(_id), isForeign(_isForeign), is_next_to_foreign(false) {}
    };

    std::unordered_map<int, Node> nodes;
    std::unordered_map<int, int> union_find;
    std::unordered_set<int> values_in_union_find;
    int local_information_id_min;
    int local_information_id_max;

    // Method to serialize the nodes into a vector
    std::vector<int> serialize() const
    {
        std::vector<int> data;

        data.push_back(local_information_id_min);
        data.push_back(local_information_id_max);

        // Serialize each node
        for (const auto &nodePair : nodes)
        {
            const Node &node = nodePair.second;

            // Serialize node ID
            data.push_back(node.id);

            // Serialize isForeign (as 0 or 1)
            data.push_back(node.isForeign ? 1 : 0);

            // Serialize the number of neighbors
            data.push_back(node.neighbors.size());

            // Serialize each neighbor
            for (int neighborId : node.neighbors)
            {
                data.push_back(neighborId);
            }
        }

        // Add a special marker at the end (e.g., -1) to indicate the end of data
        data.push_back(-1);

        return data;
    }

    void deserialize(const std::vector<int> &data)
    {
        // Clear existing data
        nodes.clear();

        size_t i = 0;
        local_information_id_min = data[i++];
        local_information_id_max = data[i++];

        std::vector<int> temp;

        while (i < data.size() && data[i] != -1)
        {
            // Deserialize node ID
            int nodeId = data[i++];

            // Deserialize isForeign
            bool isForeign = data[i++] == 1;

            // Deserialize the number of neighbors
            int numNeighbors = data[i++];

            // Add node
            addNode(nodeId, isForeign);

            // Deserialize and add each neighbor
            for (int j = 0; j < numNeighbors; ++j)
            {
                int neighborId = data[i++];
                temp.push_back(nodeId);
                temp.push_back(neighborId);
            }
        }

        for (size_t i = 0; i < temp.size(); i += 2)
        {
            addEdge(temp[i], temp[i + 1]);
        }
    }

    // Add a new node to the graph
    void addNode(int id)
    {
        if (nodes.find(id) == nodes.end())
        {
            nodes[id] = Node{id, false};
        }
    }

    void addNode(int id, bool isForeign)
    {
        if (nodes.find(id) == nodes.end())
        {
            nodes[id] = Node{id, isForeign};
        }
        else
        {
            if (nodes[id].isForeign != isForeign)
            {
                throw std::runtime_error("Node already exists with different isForeign value.");
            }
        }
    }

    // Add an edge between two nodes
    void addEdge(int from, int to)
    {
        // Check if 'from' node exists, if not, add it
        if (nodes.find(from) == nodes.end())
        {
            addNode(from);
        }

        // Check if 'to' node exists, if not, add it
        if (nodes.find(to) == nodes.end())
        {
            addNode(to);
        }

        // Now add the edge since both nodes exist
        nodes[from].neighbors.insert(to);
        nodes[to].neighbors.insert(from);
    }

    void addEdgeLocalToForeign(int from, int to)
    {
        // Check if 'from' node exists, if not, add it
        if (nodes.find(from) == nodes.end())
        {
            addNode(from);
        }

        // Check if 'to' node exists, if not, add it
        if (nodes.find(to) == nodes.end())
        {
            addNode(to, true);
        }
        else
        {
            if (!nodes[to].isForeign)
            {
                throw std::runtime_error("Node already exists with different isForeign value..");
            }
        }

        // Now add the edge since both nodes exist
        nodes[from].neighbors.insert(to);
        nodes[to].neighbors.insert(from);
    }

    bool doesNodeExist(int id) const
    {
        return nodes.find(id) != nodes.end();
    }

    bool isNodeForeign(int id) const
    {
        if (nodes.find(id) == nodes.end())
        {
            throw std::runtime_error("Node does not exist");
        }
        else
        {
            return nodes.at(id).isForeign;
        }
    }

    // Make a node local
    void makeNodeLocal(int id)
    {
        if (nodes.find(id) == nodes.end())
        {
            throw std::runtime_error("Node does not exist");
        }
        else
        {
            nodes[id].isForeign = false;
        }
    }

    // Remove an edge between two nodes
    void removeEdge(int from, int to)
    {
        nodes[from].neighbors.erase(to);
        nodes[to].neighbors.erase(from);
    }

    int find(int x)
    {
        if (union_find.find(x) == union_find.end())
        {
            return x;
        }

        while (union_find[x] != x)
        {
            x = union_find[x];
        }
        return x;
    }

    // Contract an edge between two nodes
    void contractEdge(int u, int v)
    {

        if (nodes.find(u) != nodes.end() && nodes.find(v) != nodes.end())
        {
            // Merge v's neighbors into u
            for (auto neighbor : nodes[v].neighbors)
            {
                if (neighbor != u)
                { // Avoid self-loop
                    nodes[u].neighbors.insert(neighbor);
                    nodes[neighbor].neighbors.erase(v);
                    nodes[neighbor].neighbors.insert(u);
                }
            }
            nodes[u].neighbors.erase(v);

            // Update union-find only if v is in value set (not key set)
            // if (values_in_union_find.find(v) != values_in_union_find.end()) {
            /* union_find[v] = u;
            union_find[u] = u;
            values_in_union_find.insert(u); */

            // or maybe instead
            int current = v;
            if (union_find.find(current) != union_find.end())
                while (union_find[current] != current)
                {
                    int next = union_find[current];
                    union_find[current] = u;
                    current = next;
                }

            union_find[current] = u;
            union_find[u] = u;
            values_in_union_find.insert(u);

            //}

            // Remove v from the graph
            nodes.erase(v);
        }
        else
        {
            throw std::runtime_error("One or both nodes do not exist");
        }
    }

    void contractLocalToLocalEdges()
    {
        std::vector<std::pair<int, int>> edgesToContract;

        for (auto &nodePair : nodes)
        {
            nodePair.second.is_next_to_foreign = false;
        }

        // Step 1: Collect edges to contract
        for (const auto &nodePair : nodes)
        {
            if (nodePair.second.isForeign)
            {
                for (int neighbor : nodePair.second.neighbors)
                {
                    nodes[neighbor].is_next_to_foreign = true;
                }
                continue;
            }
            int u = nodePair.first;
            for (int v : nodePair.second.neighbors)
            {
                if (u < v && !nodes[v].isForeign)
                { // Ensure each edge is only considered once
                    edgesToContract.emplace_back(u, v);
                }
            }
        }

        // Step 2: Contract edges
        for (const auto &edge : edgesToContract)
        {
            int u = find(edge.first);
            int v = find(edge.second);

            int smaller = std::min(u, v);
            int larger = std::max(u, v);

            if (smaller == larger)
            {
                continue;
                std::cout << "this case should not really happen anymore i think..." << std::endl;
            }

            if (nodes[larger].is_next_to_foreign)
            {
                continue;
            }

            contractEdge(smaller, larger);
        }
    }

    // Optional: Method to display the graph (for debugging or visualization purposes)
    void displayGraph() const
    {
        for (const auto &pair : nodes)
        {
            std::cout << "Node " << pair.first << ": ";
            for (int neighbor : pair.second.neighbors)
            {
                std::cout << neighbor << " ";
            }
            std::cout << std::endl;
        }
    }

    CAG sendAndReceive(int partner_rank)
    {
        // Serialize the current CAG instance
        std::vector<int> serializedData = serialize();

        // Determine the size of the serialized data
        int dataSize = serializedData.size();

        // Exchange the size information
        int partnerDataSize;
        MPI_Sendrecv(&dataSize, 1, MPI_INT, partner_rank, 0,
                     &partnerDataSize, 1, MPI_INT, partner_rank, 0,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Allocate space for the partner's data
        std::vector<int> partnerData(partnerDataSize);

        // Exchange the serialized data
        MPI_Sendrecv(serializedData.data(), dataSize, MPI_INT, partner_rank, 0,
                     partnerData.data(), partnerDataSize, MPI_INT, partner_rank, 0,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Deserialize the received data into a new CAG instance
        CAG newCAG;
        newCAG.deserialize(partnerData);
        return newCAG;
    }
};

class Graph
{
private:
    void DFS(int v, std::vector<bool> &visited, int label, std::vector<int> &components)
    {
        std::stack<int> stack;
        stack.push(v);

        while (!stack.empty())
        {
            v = stack.top();
            stack.pop();

            if (!visited[v - startVertexIndex])
            {
                visited[v - startVertexIndex] = true;
                components[v - startVertexIndex] = label;

                for (int i : (this->operator[](v)))
                {
                    if (i >= startVertexIndex && i < startVertexIndex + vertexCount && !visited[i - startVertexIndex])
                    {
                        stack.push(i);
                    }
                    else if (i < startVertexIndex || i >= startVertexIndex + vertexCount)
                    {
                        foreign_to_local_edges[i].push_back(v);
                        local_to_foreign_nodes[v].push_back(i);
                    }
                }
            }
        }
    }

    // std::vector<std::vector<int>> adjList;

public:
    int startVertexIndex;
    int vertexCount;
    std::vector<std::vector<int>> adjList;
    std::unordered_map<int, std::vector<int>> foreign_to_local_edges;
    std::unordered_map<int, std::vector<int>> local_to_foreign_nodes;

    /*default constructor*/
    Graph() : startVertexIndex(0), vertexCount(0) {}
    /*complete graph*/
    Graph(int vertexCount) : startVertexIndex(0), vertexCount(vertexCount), adjList(vertexCount) {}
    /*subgraph*/
    Graph(int vertexCount, int startVertexIndex) : startVertexIndex(startVertexIndex), vertexCount(vertexCount), adjList(vertexCount) {}

    // Overload the subscript operator to provide the desired offset functionality.
    std::vector<int> &operator[](size_t index)
    {
        // Adjust index by the offset.
        size_t adjustedIndex = index - startVertexIndex;

        // Check for out-of-bounds access.
        if (adjustedIndex >= adjList.size() || adjustedIndex < 0)
        {
            throw std::out_of_range("Index out of range");
        }

        return adjList[adjustedIndex];
    }

    // Const overload of the subscript operator for const contexts.
    const std::vector<int> &operator[](size_t index) const
    {
        size_t adjustedIndex = index - startVertexIndex;

        // Check for out-of-bounds access.
        if (adjustedIndex >= adjList.size() || adjustedIndex < 0)
        {
            throw std::out_of_range("Index out of range");
        }

        return adjList[adjustedIndex];
    }

    /*absolute positions*/
    void addEdge(int v, int w)
    {
        this->operator[](v).push_back(w);
        this->operator[](w).push_back(v);
    }

    /*absolute positions*/
    void addDirectedEdge(int v, int w)
    {
        this->operator[](v).push_back(w);
    }

    /*prints the current graph*/
    void print()
    {
        for (int i = 0; i < vertexCount; ++i)
        {
            std::cout << i + startVertexIndex << ": ";
            for (int w : adjList[i])
            {
                std::cout << w << " ";
            }
            std::cout << std::endl;
        }
    }

    void printWithDashes()
    {
        for (int i = 0; i < vertexCount; ++i)
        {
            for (int w : adjList[i])
            {
                if (i + startVertexIndex <= w)
                    std::cout << i + startVertexIndex << "-" << w << std::endl;
            }
        }
    }

    /*vertices contain vertex ids (absolute). "from" and "to" are the ids of the specified range of vertices (including "to").*/
    void sendSubgraph(int dest, int from, int to, MPI_Comm comm)
    {
        // Calculate the size of the buffer
        int bufferSize = sizeof(int);                // For storing the size of the vertices vector
        bufferSize += sizeof(int) * (to - from + 1); // For storing the vertices
        for (int v = from; v <= to; ++v)
        {
            if (v < startVertexIndex || v >= startVertexIndex + vertexCount)
            {
                throw std::out_of_range("vertices contains an invalid vertex id");
            }
            bufferSize += sizeof(int);                              // For storing the size of the adjacency list
            bufferSize += sizeof(int) * this->operator[](v).size(); // For storing the adjacency list
        }

        // Allocate the buffer
        char *buffer = new char[bufferSize];
        int position = 0;

        // Pack the data into the buffer
        int size = to - from + 1;
        MPI_Pack(&size, 1, MPI_INT, buffer, bufferSize, &position, comm);
        MPI_Pack(&from, 1, MPI_INT, buffer, bufferSize, &position, comm);
        for (int v = from; v <= to; ++v)
        {
            size = this->operator[](v).size();
            MPI_Pack(&size, 1, MPI_INT, buffer, bufferSize, &position, comm);
            MPI_Pack(this->operator[](v).data(), size, MPI_INT, buffer, bufferSize, &position, comm);
        }

        // Send the buffer
        MPI_Send(buffer, position, MPI_PACKED, dest, 0, comm);

        // Free the buffer
        delete[] buffer;
    }

    /*receives a graph*/
    static Graph receiveSubgraph(int source, MPI_Comm comm)
    {
        MPI_Status status;
        int bufferSize;

        // Probe for the incoming message to find out its size
        MPI_Probe(source, 0, comm, &status);
        MPI_Get_count(&status, MPI_PACKED, &bufferSize);

        // Allocate a buffer to receive the data
        char *buffer = new char[bufferSize];
        MPI_Recv(buffer, bufferSize, MPI_PACKED, source, 0, comm, &status);

        // Unpack the buffer
        int position = 0;
        int size;
        int rec_startVertexIndex;

        MPI_Unpack(buffer, bufferSize, &position, &size, 1, MPI_INT, comm);
        MPI_Unpack(buffer, bufferSize, &position, &rec_startVertexIndex, 1, MPI_INT, comm);
        Graph subgraph(size, rec_startVertexIndex);
        for (int v = rec_startVertexIndex, stop = rec_startVertexIndex + size; v < stop; ++v)
        {
            MPI_Unpack(buffer, bufferSize, &position, &size, 1, MPI_INT, comm);
            subgraph[v].resize(size);
            MPI_Unpack(buffer, bufferSize, &position, subgraph[v].data(), size, MPI_INT, comm);
        }

        // Free the buffer
        delete[] buffer;
        return subgraph;
    }

    /*creates a subgraph. "from" and "to" are the ids of the specified range of vertices (including "to").*/
    Graph createSubgraph(int from, int to)
    {
        if (from < startVertexIndex || to >= startVertexIndex + vertexCount || from > to)
        {
            throw std::out_of_range("Invalid range for subgraph creation");
        }

        int subgraphVertexCount = to - from + 1;
        Graph subgraph(subgraphVertexCount, from);

        // Adjust the indices for the new subgraph
        for (int i = from; i <= to; ++i)
        {
            for (int neighbor : this->operator[](i))
            {
                // Include only the edges where the neighbor is within the new subgraph's range
                subgraph[i].push_back(neighbor);
            }
        }

        return subgraph;
    }

    /*returns a vector containing the labels of vertices in the graph. ret[i-startVertexIndex] is label of vertex with id i*/
    std::vector<int> connectedComponents()
    {
        std::vector<bool> visited(vertexCount, false);
        std::vector<int> components(vertexCount, -1); // -1 means unvisited
        int label = startVertexIndex;

        for (int v = startVertexIndex; v < startVertexIndex + vertexCount; ++v)
        {
            if (!visited[v - startVertexIndex])
            {
                DFS(v, visited, label, components);
                label++; // Increment label for next component
            }
        }
        return components;
    }

    CAG createCAG(std::vector<int> &connectedComponents, std::unordered_map<int, int> &foreign_ID_to_label)
    {
        CAG cag;
        for (int i = startVertexIndex; i < startVertexIndex + vertexCount; ++i)
        {
            for (int j : (this->operator[](i)))
            {
                // vertex i is connected to vertex j
                if (j < startVertexIndex || j >= startVertexIndex + vertexCount)
                { // j is a foreign node
                    cag.addEdgeLocalToForeign(connectedComponents[i - startVertexIndex], foreign_ID_to_label[j]);
                }
            }
        }
        return cag;
    }
};

Graph generateRandomGraph(int num_nodes, int num_edges)
{
    Graph g(num_nodes);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, num_nodes - 1);

    for (int i = 0; i < num_edges; ++i)
    {
        int u = distrib(gen);
        int v = distrib(gen);
        if (u == v)
        {
            --i;
            continue;
        }
        g.addEdge(u, v);
    }

    return g;
}

// Function to read the 'vertices' integer from an opened HDF5 file
int readVerticesFromHDF5File(H5::H5File &file)
{
    // Open the dataset 'vertices'
    H5::DataSet datasetVertices = file.openDataSet("vertices");

    // Prepare a variable to store the vertices value
    int vertices;
    datasetVertices.read(&vertices, H5::PredType::NATIVE_INT);

    return vertices;
}

std::vector<std::vector<int>> readLinesFromHDF5(H5::H5File &file, int startLine, int endLine)
{
    std::vector<std::vector<int>> lines;

    H5::DataSet datasetLookup = file.openDataSet("lookup");
    H5::DataSet datasetData = file.openDataSet("data");

    H5::DataSpace dataspaceLookup = datasetLookup.getSpace();
    H5::DataSpace dataspaceData = datasetData.getSpace();

    // Read the lookup indices for the start and end lines
    hsize_t count[2] = {static_cast<hsize_t>(endLine - startLine + 1), 2};
    hsize_t offset[2] = {static_cast<hsize_t>(startLine), 0};
    dataspaceLookup.selectHyperslab(H5S_SELECT_SET, count, offset);
    hsize_t dimLookupRead[2] = {count[0], 2};
    H5::DataSpace memspaceLookup(2, dimLookupRead);

    std::vector<int> lookup(count[0] * 2); // Buffer for lookup indices
    datasetLookup.read(lookup.data(), H5::PredType::NATIVE_INT, memspaceLookup, dataspaceLookup);

    int startIdx = lookup[0];
    int endIdx = lookup[(endLine - startLine) * 2 + 1]; // End index of endLine

    // Read the data chunk
    hsize_t dataSize = static_cast<hsize_t>(endIdx - startIdx + 1);
    hsize_t dataOffset[1] = {static_cast<hsize_t>(startIdx)};
    dataspaceData.selectHyperslab(H5S_SELECT_SET, &dataSize, dataOffset);
    std::vector<int> chunk(dataSize);
    H5::DataSpace memspaceData(1, &dataSize);
    datasetData.read(chunk.data(), H5::PredType::NATIVE_INT, memspaceData, dataspaceData);

    // Process the data chunk
    std::vector<int> line;
    int should_be = startLine;
    bool first_number_in_line = true;
    for (int num : chunk)
    {
        if (num == -1)
        {
            lines.push_back(line);
            line.clear();
            should_be++;
            first_number_in_line = true;
        }
        else
        {
            if (first_number_in_line)
            {
                first_number_in_line = false;
                if (num != should_be)
                {
                    std::cout << "Error: " << num << " should be " << should_be << std::endl;
                    throw std::runtime_error("Error: file vertice at line " + std::to_string(num) + " should be " + std::to_string(should_be));
                }
            }
            else
            {
                line.push_back(num);
            }
        }
    }

    return lines;
}

int run(int mpi_rank, int mpi_size)
{

    int xx = 0;
    while (DEBUG_CONDITION && xx == 0 /*  && mpi_rank == RANK_OF_INTEREST */)
    {
        sleep(5);
    }

    H5::H5File file("data/coauth-DBLP-full-proj-graph-LIST_REMAP.h5", H5F_ACC_RDONLY);
    int total_vertices = readVerticesFromHDF5File(file);

    int verticesPerProcess = total_vertices / mpi_size;
    if (verticesPerProcess == 0)
    {
        throw std::runtime_error("Too many processes for amount of nodes");
    }

    int my_start_vertex_id = -1;
    int my_end_vertex_id = -1;
    if (mpi_rank == mpi_size - 1)
    {
        my_start_vertex_id = verticesPerProcess * mpi_rank;
        my_end_vertex_id = total_vertices - 1;
    }
    else
    {
        my_start_vertex_id = verticesPerProcess * mpi_rank;
        my_end_vertex_id = verticesPerProcess * (mpi_rank + 1) - 1;
    }

    Graph g_sub = Graph(my_end_vertex_id - my_start_vertex_id + 1, my_start_vertex_id);
    std::vector<std::vector<int>> lines = readLinesFromHDF5(file, my_start_vertex_id, my_end_vertex_id);
    for (size_t i = 0; i < lines.size(); ++i)
    {
        for (int neighbor : lines[i])
        {
            g_sub.addDirectedEdge(i + my_start_vertex_id, neighbor);
        }
    }

    std::vector<int> labels = g_sub.connectedComponents();

    std::vector<int> local_list; // contains the following information: [localnode, cc_id, localnode, cc_id, ...]
    for (const auto &node : g_sub.local_to_foreign_nodes)
    {
        local_list.push_back(node.first);
        local_list.push_back(labels[node.first - g_sub.startVertexIndex]);
    }

    // exchange borders with all other processes
    //  Step 1: Gather sizes of each list
    std::vector<int> list_sizes(mpi_size);
    int local_list_size = local_list.size();
    MPI_Allgather(&local_list_size, 1, MPI_INT, list_sizes.data(), 1, MPI_INT, MPI_COMM_WORLD);

    // Step 2: Compute displacements
    std::vector<int> displacements(mpi_size);
    int total_size = 0;
    for (int i = 0; i < mpi_size; ++i)
    {
        displacements[i] = total_size;
        total_size += list_sizes[i];
    }

    // Prepare a vector to receive the gathered data
    std::vector<int> gathered_list(total_size);

    // Step 3: Gather the lists
    MPI_Allgatherv(local_list.data(), local_list_size, MPI_INT,
                   gathered_list.data(), list_sizes.data(), displacements.data(),
                   MPI_INT, MPI_COMM_WORLD);

    std::unordered_map<int, int> foreign_ID_to_label;
    for (int i = 0; i < total_size; i += 2)
    { // TODO can optimize out own local nodes
        foreign_ID_to_label[gathered_list[i]] = gathered_list[i + 1];
    }

    CAG cag = g_sub.createCAG(labels, foreign_ID_to_label);

    cag.local_information_id_min = g_sub.startVertexIndex;
    cag.local_information_id_max = g_sub.startVertexIndex + g_sub.vertexCount - 1;

    for (int label : labels)
    {
        cag.union_find[label] = label;
        cag.values_in_union_find.insert(label);
    }

    // calculating reduction tree
    int partners_size = (int)(log(mpi_size) / log(2.0));
    assert((int)pow(2, partners_size) == mpi_size && "mpi_size must be power of 2 for the reduction tree (temporarily)");
    std::vector<int> partners(partners_size);

    {
        int nxt_distance = 1;
        int index = 0;
        while (nxt_distance < mpi_size)
        {
            int temp = int((mpi_rank) / nxt_distance);
            int skip = 0;

            if (temp % 2 == 1)
                skip = -nxt_distance;
            else
                skip = nxt_distance;

            partners[index++] = mpi_rank + skip;

            nxt_distance *= 2;
        }
    }

    // exchange and create
    for (int i = 0; i < partners_size; ++i)
    {
        CAG received_cag = cag.sendAndReceive(partners[i]);

        // merge received_cag into cag
        for (const auto &nodePair : received_cag.nodes)
        {
            const CAG::Node &new_cag_node = nodePair.second;

            int union_find_node_id = cag.find(new_cag_node.id);

            // Add node
            if (!cag.doesNodeExist(union_find_node_id))
            {
                // node did not exist in previous cag
                assert(union_find_node_id == new_cag_node.id);
                cag.addNode(new_cag_node.id, new_cag_node.isForeign);
                // cag.union_find[new_cag_node.id] = new_cag_node.id;
            }
            else
            {
                // node already existed in previous cag
                if (cag.isNodeForeign(union_find_node_id))
                {
                    // node was foreign in previous cag
                    if (!new_cag_node.isForeign)
                    {
                        // node is now local in current cag
                        cag.makeNodeLocal(new_cag_node.id);
                        assert(union_find_node_id == new_cag_node.id);
                        // cag.union_find[new_cag_node.id] = new_cag_node.id;
                    }
                    else
                    {
                        // node is still foreign in current cag
                        assert(cag.union_find.find(new_cag_node.id) == cag.union_find.end());
                        assert(union_find_node_id == new_cag_node.id);
                    }
                }
                else
                {
                    // node was local in previous cag
                    if (!new_cag_node.isForeign)
                    {
                        // something went wrong
                        throw std::runtime_error("Node already exists with different isForeign value...");
                    }
                    else
                    {
                        // do nothing
                    }
                }
            }

            for (int neighbor : new_cag_node.neighbors)
            {
                cag.nodes[union_find_node_id].neighbors.insert(cag.find(neighbor));
            }
        }

        std::vector<int> to_remove;
        for (const auto &nodePair : cag.nodes)
        {
            if (nodePair.first >= received_cag.local_information_id_min && nodePair.first <= received_cag.local_information_id_max && nodePair.second.isForeign)
            {
                to_remove.push_back(nodePair.first);
                for (int neighbor : nodePair.second.neighbors)
                {
                    cag.nodes[neighbor].neighbors.erase(nodePair.first);
                }
            }
        }
        for (int node_id : to_remove)
        {
            cag.nodes.erase(node_id);
        }

        // merge all local-local edges in the CAG of the variable cag
        cag.contractLocalToLocalEdges();

        assert((cag.local_information_id_max == received_cag.local_information_id_min - 1) || (cag.local_information_id_min == received_cag.local_information_id_max + 1));
        cag.local_information_id_min = std::min(cag.local_information_id_min, received_cag.local_information_id_min);
        cag.local_information_id_max = std::max(cag.local_information_id_max, received_cag.local_information_id_max);
    }

    for (size_t i = 0; i < labels.size(); ++i)
    {
        int label = labels[i];
        bool had_to_do_it = false;
        while (label != cag.union_find[label])
        {
            label = cag.union_find[label];
            had_to_do_it = true;
        }
        if (had_to_do_it)
        {
            cag.union_find[labels[i]] = label;
        }

        labels[i] = label;
    }

    if (COUNT_CC)
    {
        std::unordered_set<int> unique_labels(labels.begin(), labels.end());
        // send labels to rank 0
        if (mpi_rank != 0)
        {
            std::vector<int> labels_to_send;
            for (int label : unique_labels)
            {
                labels_to_send.push_back(label);
            }
            int labels_to_send_size = labels_to_send.size();
            MPI_Send(&labels_to_send_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(labels_to_send.data(), labels_to_send_size, MPI_INT, 0, 0, MPI_COMM_WORLD);
        }
        else
        {
            for (int i = 1; i < mpi_size; ++i)
            {
                int labels_to_receive_size;
                MPI_Recv(&labels_to_receive_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                std::vector<int> labels_to_receive(labels_to_receive_size);
                MPI_Recv(labels_to_receive.data(), labels_to_receive_size, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                for (int label : labels_to_receive)
                {
                    unique_labels.insert(label);
                }
            }
            std::cout << std::endl
                      << "Number of connected components: " << unique_labels.size() << std::endl;
        }
    }
    return 0;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int mpi_rank;
    int mpi_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    int status;
    double commulative_time = 0;
    int runs = 0;
    do
    {
        runs++;
        double start_time = MPI_Wtime();

        status = run(mpi_rank, mpi_size);

        status = 1;

        double end_time = MPI_Wtime();
        commulative_time += end_time - start_time;
        if (mpi_rank == 0)
        {
            std::cout << "time taken: " << end_time - start_time << ", average time: " << commulative_time / runs << std::endl
                      << std::endl;
        }
    } while (false);

    MPI_Finalize();
    return 0;
}