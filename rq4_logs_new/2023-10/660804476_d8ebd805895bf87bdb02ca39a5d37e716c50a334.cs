using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace breadth_first
{
    internal class Graph
    {
        public Node Root { get; set; }

        public Graph(Node root)
        {
            Root = root;
        }

        public List<Node> BreadthFirst(Node startNode)
        {
            List<Node> result = new List<Node>();
            Queue<Node> queue = new Queue<Node>();
            HashSet<Node> visited = new HashSet<Node>();

            queue.Enqueue(startNode);
            visited.Add(startNode);

            while (queue.Count > 0)
            {
                Node current = queue.Dequeue();
                result.Add(current);

                foreach (Node neighbor in current.Neighbors)
                {
                    if (!visited.Contains(neighbor))
                    {
                        queue.Enqueue(neighbor);
                        visited.Add(neighbor);
                    }
                }
            }

            return result;
        }

        public bool HasPath(Node startNode, Node endNode)
        {
            List<Node> visited = new List<Node>();
            return HasPathDFS(startNode, endNode, visited);
        }

        private bool HasPathDFS(Node currentNode, Node endNode, List<Node> visited)
        {
            if (currentNode == endNode)
                return true;

            visited.Add(currentNode);

            foreach (Node neighbor in currentNode.Neighbors)
            {
                if (!visited.Contains(neighbor) && HasPathDFS(neighbor, endNode, visited))
                    return true;
            }

            return false;
        }
    }
}