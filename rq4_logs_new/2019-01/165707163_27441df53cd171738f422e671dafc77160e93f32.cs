using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace Trees.Classes
{
    public class BinarySearchTree
    {
        public Node Root { get; set; }

        public Node Add(Node node)
        {
            Queue<Node> queue = new Queue<Node>();
            Node check;
            Node current = Root;
            if (!queue.TryDequeue(out check))
                queue.Enqueue(node);
            while(queue.TryDequeue(out check))
            {
                Node root = queue.Dequeue();
                current = root;
                queue.Enqueue(root.Left);
                queue.Enqueue(root.Right);
            }
            if (node.Value > current.Value)
                current.Right = node;
            if (node.Value < current.Value || node.Value == current.Value)
                current.Left = node;
            return Root;
        }
        public bool Contains(Node node) { return true; }
    }
}