using System;
using System.Collections.Generic;
using System.Text;

namespace AutomataViaGraph.Graph.Default
{
    public class UndirectedGraph<T> : IGraph<T>
    {
        public IEnumerable<IEdge<T>> Edges { get { return edges; } }
        public IEnumerable<IVertex<T>> Vertices { get { return vertices; } }
        private List<IEdge<T>> edges = new List<IEdge<T>>();
        private List<IVertex<T>> vertices = new List<IVertex<T>>();
        public void Add(T data)
        {
            throw new NotImplementedException();
        }

        public void AddEdge(T vertex1, T vertex2)
        {
            throw new NotImplementedException();
        }

        public void Remove(T data)
        {
            throw new NotImplementedException();
        }
    }
}