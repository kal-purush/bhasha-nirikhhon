using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Graph
{
    public class Graphs
    {
        private Dictionary<string, Vertex> vertices;
        private Dictionary<Vertex, List<Edge>> adjacencyList;

        public Graphs()
        {
            vertices = new Dictionary<string, Vertex>();
            adjacencyList = new Dictionary<Vertex, List<Edge>>();
        }

        public Vertex AddVertex(string value)
        {
            if (!vertices.ContainsKey(value))
            {
                var vertex = new Vertex(value);
                vertices[value] = vertex;
                adjacencyList[vertex] = new List<Edge>();
                return vertex;
            }
            throw new ArgumentException(" already exists.");
        }

        public Dictionary<Vertex, List<Edge>> AddEdge(Vertex a, Vertex b, int weight = 0)
        {
            if (!vertices.ContainsValue(a) || !vertices.ContainsValue(b))
            {
                throw new ArgumentException("already be in the graph.");
            }

            adjacencyList[a].Add(new Edge(b, weight));
            adjacencyList[b].Add(new Edge(a, weight));
            return adjacencyList;
        }

        public ICollection<Vertex> GetVertices()
        {
            return vertices.Values;
        }

        public ICollection<Edge> GetNeighbors(Vertex vertex)
        {
            if (adjacencyList.ContainsKey(vertex))
            {
                return adjacencyList[vertex];
            }
            return new List<Edge>();
        }

        public int Size()
        {
            return vertices.Count;
        }

        public class Vertex
        {
            public string Value { get; }

            public Vertex(string value)
            {
                Value = value;
            }
        }

        public class Edge
        {
            public Vertex Vertex { get; }
            public int Weight { get; }

            public Edge(Vertex target, int weight)
            {
                Vertex = target;
                Weight = weight;
            }
        }
        public void Print()
        {
            foreach (var item in adjacencyList)
            {
                Console.Write($"Vertex {item.Key.Value} =>");

                foreach (var edge in item.Value)
                {
                    Console.Write($"{edge.Vertex.Value} =>");
                }

                Console.WriteLine();
            }
        }
    }
}