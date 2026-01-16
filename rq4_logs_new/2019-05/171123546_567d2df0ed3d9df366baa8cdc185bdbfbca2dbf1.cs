using System;
using System.Collections.Generic;

namespace Set
{
    public class Set<T> : ISet<T> where T : IComparable<T>
    {
        public int Count { get; private set; }

        private Node root;

        public class Node
        {
            public T Key { get; private set; }
            public Node Left { get; set; }
            public Node Right { get; set; }

            public Node(T value)
            {
                Key = value;
            }
        }

        public bool IsReadOnly => false;

        private Node GetNodeByKey(Node node, T key)
        {
            if (key.CompareTo(node.Key) > 0)
            {
                if (node.Right == null)
                {
                    return null;
                }
                GetNodeByKey(node.Right, key);
            }
            else if (key.CompareTo(node.Key) < 0)
            {
                if (node.Left == null)
                {
                    return null;
                }
                GetNodeByKey(node.Left, key);
            }
            return node;
        }

        public bool Contains(T key)
        {
            if (Count == 0)
            {
                return false;
            }
            return GetNodeByKey(root, key) != null;
        }

        private void DoAdd(Node node, T key)
        {
            if (key.CompareTo(node.Key) > 0)
            {
                if (node.Right == null)
                {
                    node.Right = new Node(key);
                    return;
                }
                DoAdd(node.Right, key);
            }
            if (key.CompareTo(node.Key) < 0)
            {
                if (node.Left == null)
                {
                    node.Left = new Node(key);
                    return;
                }
                DoAdd(node.Right, key);
            }
        }

        public bool Add(T key)
        {
            if (Count == 0)
            {
                root = new Node(key);
                ++Count;
                return true;
            }
            if (Contains(key))
            {
                return false;
            }
            DoAdd(root, key);
            ++Count;
            return true;
        }

        private void DoRemove(Node node, T key)
        {
            
        }

        public bool Remove(T key)
        {
            if (Count == 0)
            {
                return false;
            }
            if (!Contains(key))
            {
                return false;
            }
            DoRemove(root, key);
            --Count;
            return true;
        }

        public void Clear()
        {
            root = null;
            Count = 0;
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            foreach (var key in other)
            {
                if (Contains(key))
                {
                    return true;
                }
            }
            return false;
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            var intersection = new List<T>();
            foreach (var key in other)
            {
                if (Contains(key))
                {
                    intersection.Add(key);
                }
            }
            Clear();
            foreach (var key in intersection)
            {
                Add(key);
            }
        }

        public void ExceptWith(IEnumerable<T> other)
        {
            foreach (var key in other)
            {
                Remove(key);
            }
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            Clear();
            UnionWith(other);
        }

        public void UnionWith(IEnumerable<T> other)
        {
            foreach (var key in other)
            {
                Add(key);
            }
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            
        }
    }
}