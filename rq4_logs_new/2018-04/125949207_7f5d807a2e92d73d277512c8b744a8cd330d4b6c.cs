using System;
using System.Collections.Generic;
using System.Text;

namespace BinaryTree
{
    public class Tree<T>
    {
        public Node<T> Root { get; set; }

        public Tree(T rootValue)
        {
            Root = new Node<T>(rootValue);
        }

        public List<T> PreOrderTraversal(Node<T> node, List<T> values)
        {
            values.Add(node.Value);

            if (node.Left != null)
            {
                PreOrderTraversal(node.Left, values);
            }

            if (node.Right != null)
            {
                PreOrderTraversal(node.Right, values);
            }

            return values;
        }

        public List<T> InOrderTraversal(Node<T> node, List<T> values)
        {
            if (node.Left != null)
            {
                InOrderTraversal(node.Left, values);
            }

            values.Add(node.Value);

            if (node.Right != null)
            {
                InOrderTraversal(node.Right, values);
            }

            return values;
        }

        public List<T> PostOrderTraversal(Node<T> node, List<T> values)
        {
            if (node.Left != null)
            {
                PostOrderTraversal(node.Left, values);
            }

            if (node.Right != null)
            {
                PostOrderTraversal(node.Right, values);
            }

            values.Add(node.Value);

            return values;
        }
    }
}