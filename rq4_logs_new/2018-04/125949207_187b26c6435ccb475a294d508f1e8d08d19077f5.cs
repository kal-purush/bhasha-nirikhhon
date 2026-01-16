using StackAndQueue;
using System;
using System.Collections.Generic;
using System.Text;

namespace KAryTree
{
    public class KAryTree<T>
    {
        public KAryNode<T> Root { get; internal set; }
        public int Count { get; private set; } = 0;

        /// <summary>
        /// Constructor for KAryTree. Creates a new KAryNode at the root of the
        /// tree containing the specified rootValue and no children.
        /// </summary>
        /// <param name="rootValue">The value to be held by the tree's root node</param>
        public KAryTree(T rootValue)
        {
            Root = new KAryNode<T>() { Value = rootValue };

            if (rootValue != null)
            {
                Count++;
            }
        }

        // TODO(taylorjoshuaw): Add an empty constructor if the caller does not want
        //                      to initialize the tree with any nodes. Must add in
        //                      a method that can replace root before this can be
        //                      implemented (otherwise no nodes could be added).

        /// <summary>
        /// Perform breadth first traversal, adding each visited node to an IEnumerable
        /// in the order that those nodes were visited.
        /// </summary>
        /// <returns>IEnumerable of all values held in the tree in breadth-first order</returns>
        public IEnumerable<T> BreadthFirstTraversal()
        {
            MyQueue<KAryNode<T>> nodeQueue = new MyQueue<KAryNode<T>>(Root);
            List<T> values = new List<T>();

            while (nodeQueue.Length > 0)
            {
                KAryNode<T> currentNode = nodeQueue.Dequeue();
                values.Add(currentNode.Value);
                
                foreach (KAryNode<T> node in currentNode.Children)
                {
                    nodeQueue.Enqueue(node);
                }
            }

            return values;
        }

        /// <summary>
        /// Searches for a node with the provided value within the tree and returns
        /// it if found; otherwise, null is returned.
        /// </summary>
        /// <param name="searchValue">The value to search for within the tree nodes' values</param>
        /// <returns>A reference to the node containing the specified value. Returns null
        /// if no node within the tree contains the specified value.</returns>
        public KAryNode<T> Search(T searchValue)
        {
            MyQueue<KAryNode<T>> nodeQueue = new MyQueue<KAryNode<T>>(Root);

            while (nodeQueue.Length > 0)
            {
                KAryNode<T> currentNode = nodeQueue.Dequeue();
                
                // If the current node has the specified value, return the node as a match
                if (currentNode.Value.Equals(searchValue))
                {
                    return currentNode;
                }
                
                foreach (KAryNode<T> node in currentNode.Children)
                {
                    nodeQueue.Enqueue(node);
                }
            }

            // No node was found with the specified value. Return null.
            return null;
        }

        /// <summary>
        /// Attempts to add a new node to the tree containing newValue with the first
        /// node (breadth-first) containing parentValue as its parent. Returns true
        /// on success or false if no nodes containing parentValue were found.
        /// </summary>
        /// <param name="newValue">The value contained by the new node</param>
        /// <param name="parentValue">The value contained by the new node's parent</param>
        /// <returns>True if a node was added. False if no nodes containing parentValue
        /// could be found in the tree.</returns>
        public bool Add(T newValue, T parentValue)
        {
            KAryNode<T> parent = Search(parentValue);

            // If no nodes were found matching parentValue then return false
            // to indicate that no new nodes were added
            if (parent is null)
            {
                return false;
            }

            // Add a new node containing newValue and return true to indicate success
            parent.Children.Add(new KAryNode<T>() { Value = newValue });
            Count++;

            return true;
        }
    }
}