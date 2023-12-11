#define _GLIBCXX_DEBUG
#include <vector>
#include <iostream>
#include <mpi.h>
#include <algorithm>
#include <unistd.h>
#include <cassert>
#include <string>
#include <unordered_map>
#include <unordered_set>

#define DEBUG_CONDITION false
#define RANK_OF_INTEREST 1

/// @brief

class CAG {
    public: 
    struct Node {
        int id;
        bool isForeign;
        std::unordered_set<int> neighbors; // Using unordered_set to avoid duplicates
        Node() : id(-1), isForeign(false) {} // Default constructor
        Node(int _id, bool _isForeign) : id(_id), isForeign(_isForeign) {}
    };

    std::unordered_map<int, Node> nodes;
    std::unordered_map<int, int> union_find;
    std::unordered_set<int> values_in_union_find;

    // Method to serialize the nodes into a vector
    std::vector<int> serialize() const {
        std::vector<int> data;

        // Serialize each node
        for (const auto& nodePair : nodes) {
            const Node& node = nodePair.second;

            // Serialize node ID
            data.push_back(node.id);

            // Serialize isForeign (as 0 or 1)
            data.push_back(node.isForeign ? 1 : 0);

            // Serialize the number of neighbors
            data.push_back(node.neighbors.size());

            // Serialize each neighbor
            for (int neighborId : node.neighbors) {
                data.push_back(neighborId);
            }
        }

        // Add a special marker at the end (e.g., -1) to indicate the end of data
        data.push_back(-1);

        return data;
    }

    void deserialize(const std::vector<int>& data) {
        // Clear existing data
        nodes.clear();

        size_t i = 0;

        std::vector<int> temp;

        while (i < data.size() && data[i] != -1) {
            // Deserialize node ID
            int nodeId = data[i++];

            // Deserialize isForeign
            bool isForeign = data[i++] == 1;

            // Deserialize the number of neighbors
            int numNeighbors = data[i++];

            // Add node
            addNode(nodeId, isForeign);

            // Deserialize and add each neighbor
            for (int j = 0; j < numNeighbors; ++j) {
                int neighborId = data[i++];
                temp.push_back(nodeId);
                temp.push_back(neighborId);
            }
        }

        for (size_t i = 0; i < temp.size(); i += 2) {
            addEdge(temp[i], temp[i + 1]);
        }
    }

    // Add a new node to the graph
    void addNode(int id) {
        if (nodes.find(id) == nodes.end()) {
            nodes[id] = Node{id, false};
        }
    }

    void addNode(int id, bool isForeign) {
        if (nodes.find(id) == nodes.end()) {
            nodes[id] = Node{id, isForeign};
        }else{
            if (nodes[id].isForeign != isForeign){
                throw std::runtime_error("Node already exists with different isForeign value.");
            }
        }
    }

    // Add an edge between two nodes
    void addEdge(int from, int to) {
        // Check if 'from' node exists, if not, add it
        if (nodes.find(from) == nodes.end()) {
            addNode(from);
        }

        // Check if 'to' node exists, if not, add it
        if (nodes.find(to) == nodes.end()) {
            addNode(to);
        }

        // Now add the edge since both nodes exist
        nodes[from].neighbors.insert(to);
        nodes[to].neighbors.insert(from);
    }

    void addEdgeLocalToForeign(int from, int to) {
        // Check if 'from' node exists, if not, add it
        if (nodes.find(from) == nodes.end()) {
            addNode(from);
        }

        // Check if 'to' node exists, if not, add it
        if (nodes.find(to) == nodes.end()) {
            addNode(to, true);
        }else{
            if (!nodes[to].isForeign){
                throw std::runtime_error("Node already exists with different isForeign value..");
            }
        }

        // Now add the edge since both nodes exist
        nodes[from].neighbors.insert(to);
        nodes[to].neighbors.insert(from);
    }

    bool doesNodeExist(int id) const {
        return nodes.find(id) != nodes.end();
    }

    bool isNodeForeign(int id) const {
        if (nodes.find(id) == nodes.end()) {
            throw std::runtime_error("Node does not exist");
        }else{
            return nodes.at(id).isForeign;
        }
    }

    // Make a node local
    void makeNodeLocal(int id){
        if (nodes.find(id) == nodes.end()) {
            throw std::runtime_error("Node does not exist");
        }else{
            nodes[id].isForeign = false;
        }
    }

    // Remove an edge between two nodes
    void removeEdge(int from, int to) {
        nodes[from].neighbors.erase(to);
        nodes[to].neighbors.erase(from);
    }

    int find(int x) {
        if (union_find.find(x) == union_find.end()) {
            return x;
        }

        while (union_find[x] != x) {
            x = union_find[x];
        }
        return x;
    }

    // Contract an edge between two nodes
    void contractEdge(int u, int v) {
        int smaller = -1;
        int larger = -1;
        if (u < v) {
            smaller = u;
            larger = v;
        }else if (v < u) {
            smaller = v;
            larger = u;
        }else{
            throw std::runtime_error("Cannot contract an edge between a node and itself");
        }

        if (nodes.find(smaller) != nodes.end() && nodes.find(larger) != nodes.end()) {
            // Merge v's neighbors into u
            for (auto neighbor : nodes[larger].neighbors) {
                if (neighbor != smaller) { // Avoid self-loop
                    nodes[smaller].neighbors.insert(neighbor);
                    nodes[neighbor].neighbors.erase(larger);
                    nodes[neighbor].neighbors.insert(smaller);
                }
            }
            nodes[smaller].neighbors.erase(larger);

            // Update union-find only if v is in value set (not key set)
            //if (values_in_union_find.find(larger) != values_in_union_find.end()) {
                /* union_find[larger] = smaller;
                union_find[smaller] = smaller;
                values_in_union_find.insert(smaller); */

                // or maybe instead
                int current = larger;
                if (union_find.find(current) != union_find.end())
                    while (union_find[current] != current) {
                        int next = union_find[current];
                        union_find[current] = smaller;
                        current = next;
                    }

                union_find[current] = smaller;
                union_find[smaller] = smaller;
                values_in_union_find.insert(smaller);


            //}

            // Remove v from the graph
            nodes.erase(larger);
        }else{
            throw std::runtime_error("One or both nodes do not exist");
        }
    }

    void contractLocalToLocalEdges() {
        std::vector<std::pair<int, int>> edgesToContract;

        // Step 1: Collect edges to contract
        for (const auto& nodePair : nodes) {
            if (nodePair.second.isForeign) {
                continue;
            }
            int u = nodePair.first;
            for (int v : nodePair.second.neighbors) {
                if (u < v && !nodes[v].isForeign){ // Ensure each edge is only considered once
                    edgesToContract.emplace_back(u, v);
                }
            }
        }

        // Step 2: Contract edges
        for (const auto& edge : edgesToContract) {
            contractEdge(find(edge.first), find(edge.second));
        }
    }

    // Optional: Method to display the graph (for debugging or visualization purposes)
    void displayGraph() const {
        for (const auto& pair : nodes) {
            std::cout << "Node " << pair.first << ": ";
            for (int neighbor : pair.second.neighbors) {
                std::cout << neighbor << " ";
            }
            std::cout << std::endl;
        }
    }

    CAG sendAndReceive(int partner_rank) {
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
    void DFS(int v, std::vector<bool> &visited, int label, std::vector<int> &components) {
        visited[v-startVertexIndex] = true;
        components[v-startVertexIndex] = label;

        // Recur for all the vertices adjacent to this vertex
        for (int i : (this->operator[](v))) {
            if (i >= startVertexIndex && i < startVertexIndex+vertexCount && !visited[i-startVertexIndex]) {
                DFS(i, visited, label, components);
            }else if(i < startVertexIndex || i >= startVertexIndex+vertexCount){
                foreign_to_local_edges[i].push_back(v);
                local_to_foreign_nodes[v].push_back(i);
            }
        }
    }

    //std::vector<std::vector<int>> adjList;

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

    /*vertices contain vertex ids (absolute). "from" and "to" are the ids of the specified range of vertices (including "to").*/
    void sendSubgraph(int dest, int from, int to, MPI_Comm comm)
    {
        // Calculate the size of the buffer
        int bufferSize = sizeof(int);                      // For storing the size of the vertices vector
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
    Graph createSubgraph(int from, int to) {
        if (from < startVertexIndex || to >= startVertexIndex + vertexCount || from > to) {
            throw std::out_of_range("Invalid range for subgraph creation");
        }

        int subgraphVertexCount = to - from + 1;
        Graph subgraph(subgraphVertexCount, from);

        // Adjust the indices for the new subgraph
        for (int i = from; i <= to; ++i) {
            for (int neighbor : this->operator[](i)) {
                // Include only the edges where the neighbor is within the new subgraph's range
                subgraph[i].push_back(neighbor);
            }
        }

        return subgraph;
    }

    /*returns a vector containing the labels of vertices in the graph. ret[i-startVertexIndex] is label of vertex with id i*/
    std::vector<int> connectedComponents() {
        std::vector<bool> visited(vertexCount, false);
        std::vector<int> components(vertexCount, -1); // -1 means unvisited
        int label = startVertexIndex;

        for (int v = startVertexIndex; v < startVertexIndex+vertexCount; ++v) {
            if (!visited[v-startVertexIndex]) {
                DFS(v, visited, label, components);
                label++; // Increment label for next component
            }
        }
        return components;
    }

    CAG createCAG(std::vector<int>& connectedComponents, std::unordered_map<int, int>& foreign_ID_to_label){
        CAG cag;
        for (int i = startVertexIndex; i < startVertexIndex+vertexCount; ++i){
            for (int j : (this->operator[](i))){
                // vertex i is connected to vertex j
                if (j < startVertexIndex || j >= startVertexIndex+vertexCount){ // j is a foreign node
                    cag.addEdgeLocalToForeign(connectedComponents[i-startVertexIndex], foreign_ID_to_label[j]);
                }
            }
        }
        return cag;
    }

};


int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);



    int mpi_rank;
    int mpi_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    int xx = 0;
    while (DEBUG_CONDITION && xx == 0 && mpi_rank == RANK_OF_INTEREST)
    {
        sleep(5);
    }

    Graph g_sub;

    if (mpi_rank == 0)
    {
        /* Graph g(138);
        {
            g.addEdge(0, 1);
            g.addEdge(1, 2);
            g.addEdge(2, 3);
            g.addEdge(3, 4);
            g.addEdge(4, 5);
            g.addEdge(5, 6);
            g.addEdge(6, 7);
            g.addEdge(7, 8);
            g.addEdge(8, 9);
            g.addEdge(9, 10);
            g.addEdge(10, 11);
            g.addEdge(12, 13);
            g.addEdge(13, 14);
            g.addEdge(14, 15);
            g.addEdge(15, 16);
            g.addEdge(16, 17);
            g.addEdge(17, 18);
            g.addEdge(18, 19);
            g.addEdge(19, 20);
            g.addEdge(19, 59);
            g.addEdge(20, 21);
            g.addEdge(21, 22);
            g.addEdge(22, 23);
            g.addEdge(24, 25);
            g.addEdge(25, 26);
            g.addEdge(26, 27);
            g.addEdge(27, 28);
            g.addEdge(28, 29);
             g.addEdge(29, 30);
            g.addEdge(30, 31);
            g.addEdge(31, 32);
            g.addEdge(32, 33);
            g.addEdge(33, 34);
            g.addEdge(33, 79);
            g.addEdge(34, 35);
             g.addEdge(35, 115);
             g.addEdge(36, 37);
            g.addEdge(37, 38);
            g.addEdge(38, 39);
            g.addEdge(39, 40);
            g.addEdge(40, 41);
            g.addEdge(41, 42);
            g.addEdge(42, 43);
            g.addEdge(43, 44);
            g.addEdge(44, 45);
            g.addEdge(45, 46);
            g.addEdge(46, 47);
            g.addEdge(48, 49);
            g.addEdge(49, 50);
            g.addEdge(50, 51);
            g.addEdge(51, 52);
            g.addEdge(52, 53);
            g.addEdge(53, 54);
            g.addEdge(54, 55);
            g.addEdge(55, 56);
            g.addEdge(56, 57);
            g.addEdge(57, 58);
            g.addEdge(58, 59);
            g.addEdge(60, 61);
            g.addEdge(61, 62);
            g.addEdge(62, 63);
            g.addEdge(63, 64);
            g.addEdge(64, 65);
            g.addEdge(65, 66);
            g.addEdge(66, 67);
            g.addEdge(67, 68);
            g.addEdge(67, 76);
            g.addEdge(68, 69);
            g.addEdge(69, 70);
            g.addEdge(70, 71);
            g.addEdge(72, 73);
            g.addEdge(73, 74);
            g.addEdge(74, 75);
            g.addEdge(75, 76);
            g.addEdge(76, 77);
            g.addEdge(77, 78);
            g.addEdge(78, 79);
            g.addEdge(79, 80);
            g.addEdge(80, 81);
            g.addEdge(81, 82);
            g.addEdge(83, 84);
            g.addEdge(84, 85);
            g.addEdge(85, 86);
            g.addEdge(86, 87);
            g.addEdge(87, 88);
            g.addEdge(88, 89);
            g.addEdge(89, 90);
            g.addEdge(90, 91);
            g.addEdge(91, 92);
            g.addEdge(92, 93);
            g.addEdge(94, 95);
            g.addEdge(95, 96);
            g.addEdge(96, 97);
            g.addEdge(97, 98);
            g.addEdge(98, 99);
            g.addEdge(99, 100);
            g.addEdge(100, 101);
            g.addEdge(101, 102);
            g.addEdge(102, 103);
            g.addEdge(103, 104);
            g.addEdge(105, 106);
            g.addEdge(106, 107);
            g.addEdge(107, 108);
            g.addEdge(108, 109);
            g.addEdge(109, 110);
            g.addEdge(110, 111);
            g.addEdge(111, 112);
            g.addEdge(112, 113);
            g.addEdge(113, 114);
            g.addEdge(114, 115);
            g.addEdge(116, 117);
            g.addEdge(116, 121);
            g.addEdge(117, 118);
            g.addEdge(118, 119);
            g.addEdge(119, 120);
            g.addEdge(120, 121);
            g.addEdge(121, 122);
            g.addEdge(122, 123);
            g.addEdge(123, 124);
            g.addEdge(124, 125);
            g.addEdge(125, 126);
            g.addEdge(127, 128);
            g.addEdge(128, 129);
            g.addEdge(129, 130);
            g.addEdge(130, 131);
            g.addEdge(131, 132);
            g.addEdge(132, 133);
            g.addEdge(133, 134);
            g.addEdge(134, 135);
            g.addEdge(135, 136);
            g.addEdge(136, 137);
        }
         */
        /* Graph g(12);
        {
            g.addEdge(0,1);
            g.addEdge(1,2);
            g.addEdge(3,4);
            g.addEdge(5,6);
            g.addEdge(7,8);
            g.addEdge(0,9);
            g.addEdge(9,10);
            g.addEdge(10,11);
            g.addEdge(2,11);

        } */


/* Graph g(12);
    {
        g.addEdge(9,10);
        g.addEdge(2,4);
        g.addEdge(3,8);
        g.addEdge(10,11);
        g.addEdge(6,11);
        g.addEdge(7,10);
        g.addEdge(1,9);
        g.addEdge(0,3);
        g.addEdge(8,10);
        g.addEdge(11,3);
        g.addEdge(3,10);
        g.addEdge(5,6);
        g.addEdge(6,10);
        g.addEdge(4,7);
        g.addEdge(2,8);
    } */
 /*    Graph g(6);
    {
        g.addEdge(0,3);
        g.addEdge(3,1);
        g.addEdge(1,4);
        g.addEdge(4,2);
        g.addEdge(2,5);
    }
 */
Graph g(8);
    {
        g.addEdge(0,1);
        g.addEdge(1,2);
        g.addEdge(2,3);
        g.addEdge(3,4);
        g.addEdge(4,5);
        g.addEdge(5,6);
        g.addEdge(6,7);
    }

        int verticesPerProcess = g.vertexCount / mpi_size;
        if (verticesPerProcess == 0)
        {
            throw std::runtime_error("Too many processes");
        }

        for (int receiver = 1; receiver < mpi_size-1; ++receiver)
        {
            g.sendSubgraph(receiver, verticesPerProcess*receiver, verticesPerProcess*(receiver+1)-1, MPI_COMM_WORLD);
        }
        g.sendSubgraph(mpi_size-1, verticesPerProcess*(mpi_size-1), g.vertexCount-1, MPI_COMM_WORLD);

        
        g_sub = g.createSubgraph(0, verticesPerProcess-1);
    }
    else
    {
        g_sub = Graph::receiveSubgraph(0, MPI_COMM_WORLD);
    }
    
    sleep(mpi_rank);
    std::vector<int> labels = g_sub.connectedComponents();

    std::vector<int> local_list; // contains the following information: [localnode, cc_id, localnode, cc_id, ...]
    for (const auto& node : g_sub.local_to_foreign_nodes) {
        local_list.push_back(node.first);
        local_list.push_back(labels[node.first-g_sub.startVertexIndex]);
    }

    //exchange borders with all other processes
    // Step 1: Gather sizes of each list
    std::vector<int> list_sizes(mpi_size);
    int local_list_size = local_list.size();
    MPI_Allgather(&local_list_size, 1, MPI_INT, list_sizes.data(), 1, MPI_INT, MPI_COMM_WORLD);

    // Step 2: Compute displacements
    std::vector<int> displacements(mpi_size);
    int total_size = 0;
    for (int i = 0; i < mpi_size; ++i) {
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
    for (int i = 0; i < total_size; i += 2) { // TODO can optimize out own local nodes
        foreign_ID_to_label[gathered_list[i]] = gathered_list[i + 1];
    }

    CAG cag = g_sub.createCAG(labels, foreign_ID_to_label);
    
    
    for (int label : labels) {
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
        while (nxt_distance < mpi_size) {
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
    for (int i = 0; i < partners_size; ++i) {
        CAG received_cag = cag.sendAndReceive(partners[i]);


        // merge received_cag into cag
        for (const auto& nodePair : received_cag.nodes) {
            const CAG::Node& new_cag_node = nodePair.second;

            // Add node
            if (!cag.doesNodeExist(new_cag_node.id)) {
                // node did not exist in previous cag
                cag.addNode(new_cag_node.id, new_cag_node.isForeign);
                //cag.union_find[new_cag_node.id] = new_cag_node.id;
            }else{
                // node already existed in previous cag
                if (cag.isNodeForeign(new_cag_node.id)){
                    // node was foreign in previous cag
                    if (!new_cag_node.isForeign){
                        // node is now local in current cag
                        cag.makeNodeLocal(new_cag_node.id);
                        //cag.union_find[new_cag_node.id] = new_cag_node.id;
                    }else{
                        // node is still foreign in current cag
                    }
                }else{
                    // node was local in previous cag
                    if (!new_cag_node.isForeign){
                        // something went wrong
                        throw std::runtime_error("Node already exists with different isForeign value...");
                    }else{
                        // do nothing
                    }
                }
            }

            for (int neighbor : new_cag_node.neighbors) {
                cag.nodes[new_cag_node.id].neighbors.insert(neighbor);
            }
        }

        // merge all local-local edges in the CAG of the variable cag
        cag.contractLocalToLocalEdges();

    }


    for (size_t i = 0; i < labels.size(); ++i) {
        int label = labels[i];
        bool had_to_do_it = false;
        while (label != cag.union_find[label]) {
            label = cag.union_find[label];
            had_to_do_it = true;
        }
        if (had_to_do_it){
            cag.union_find[labels[i]] = label;
        }

        labels[i] = label;
    }

    for (int i = g_sub.startVertexIndex; i < g_sub.startVertexIndex+g_sub.vertexCount; ++i) {
        std::cout << i << " belongs to label " << labels[i-g_sub.startVertexIndex] << std::endl;
    }


            while (DEBUG_CONDITION && mpi_rank != RANK_OF_INTEREST)
            {
                sleep(5);
            }




    MPI_Finalize();
    return 0;
}
