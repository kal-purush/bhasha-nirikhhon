using System;

namespace Trees
{
    public class BinarySearchTree<T> : BinaryTree<T> where T : IComparable
    {
        public void Add(T value)
        {
            Root = AddNode(Root, value);
        }

        public Node<T> AddNode(Node<T> node, T value)
        {
            if (node == null)
                return new Node<T>(value);

            if (value.CompareTo(node.Value) > 0)
                node.Right = AddNode(node.Right, value);
            else if (value.CompareTo(node.Value) < 0)
                node.Left = AddNode(node.Left, value);

            return node;
        }

        public bool Contains(T value)
        {
            return Contains(Root, value);
        }

        private bool Contains(Node<T> node, T value)
        {
            if (node == null)
            {
                return false;
            }

            if (node.Value.Equals(value))
            {
                return true;
            }
            else if (value.CompareTo(node.Value) < 0)
            {
                return Contains(node.Left, value);
            }
            else
            {
                return Contains(node.Right, value);
            }
        }
    }
}