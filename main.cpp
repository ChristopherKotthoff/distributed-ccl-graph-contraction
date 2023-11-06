#define _GLIBCXX_DEBUG
#include <vector>
#include <iostream>
#include <mpi.h>
#include <algorithm>
#include <unistd.h>
#include <cassert>

/// @brief
class Graph
{
private:

public:
    int startVertexIndex;
    int vertexCount;
    std::vector<std::vector<int>> adjList;

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
                if (neighbor >= from && neighbor <= to) {
                    subgraph[i].push_back(neighbor);
                }
            }
        }

        return subgraph;
    }

    void DFS(int v, std::vector<bool> &visited, int label, std::vector<int> &components) {
        visited[v-startVertexIndex] = true;
        components[v-startVertexIndex] = label;

        // Recur for all the vertices adjacent to this vertex
        for (int i : (this->operator[](v))) {
            if (i >= startVertexIndex && i < startVertexIndex+vertexCount && !visited[i-startVertexIndex]) {
                DFS(i, visited, label, components);
            }
        }
    }

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

};

int main()
{

    int xx = 1;
    while (xx == 0)
    {
        sleep(5);
    }

    MPI_Init(nullptr, nullptr);
    int mpi_rank;
    int mpi_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    Graph g_sub;

    if (mpi_rank == 0)
    {
        Graph g(138);
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
    for (int i = 0; i < g_sub.vertexCount; ++i)
        {
            std::cout << i + g_sub.startVertexIndex << ": ";
            for (int w : g_sub.adjList[i])
            {
                std::cout << w << " ";
            }
            std::cout << "and has label: " << labels[i]<< std::endl;
        }
    std::cout << std::endl;

    MPI_Finalize();
    return 0;
}
