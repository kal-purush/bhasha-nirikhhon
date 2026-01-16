using BinaryTree;
using StackAndQueue;
using System;
using System.Collections.Generic;

namespace FindMaximumValueBinaryTree
{
    class Program
    {
        static void Main(string[] args)
        {
            Tree<int> tree = ConstructTestTree();
            List<int> treeValues = new List<int>();

            tree.InOrderTraversal(tree.Root, treeValues);

            Console.WriteLine("In order traversal of demonstration tree:");
            Console.WriteLine($"[{string.Join(", ", treeValues)}]");
            Console.WriteLine();

            Console.WriteLine($"Maximum value found via FindMaximumValue(): {FindMaximumValue(tree)}");
            Console.WriteLine();

            Console.WriteLine("Please press any key to exit this program...");
            Console.ReadKey();
        }

        /// <summary>
        /// Provides an example tree from which the demonstration of the FindMaximumValue
        /// method can be performed
        /// </summary>
        /// <returns>A tree with nodes matching those from the challenge example</returns>
        public static Tree<int> ConstructTestTree()
        {
            Tree<int> tree = new Tree<int>(2);

            BinaryTree.Node<int> left = tree.Root.Left = new BinaryTree.Node<int>(7);
            BinaryTree.Node<int> right = tree.Root.Right = new BinaryTree.Node<int>(5);

            BinaryTree.Node<int> leftLeft = left.Left = new BinaryTree.Node<int>(2);
            BinaryTree.Node<int> leftRight = left.Right = new BinaryTree.Node<int>(6);

            BinaryTree.Node<int> leftRightleft = leftRight.Left = new BinaryTree.Node<int>(5);
            BinaryTree.Node<int> leftRightRight = leftRight.Right = new BinaryTree.Node<int>(11);

            BinaryTree.Node<int> rightRight = right.Right = new BinaryTree.Node<int>(9);
            BinaryTree.Node<int> rightRightLeft = rightRight.Left = new BinaryTree.Node<int>(4);

            return tree;
        }

        /// <summary>
        /// Given a binary tree holding int nodes, returns the maximum value held in that tree
        /// </summary>
        /// <param name="tree">The tree to check the maximum value for</param>
        /// <returns>The maximum value held by the nodes within the input tree</returns>
        public static int FindMaximumValue(Tree<int> tree)
        {
            int maximumValue = int.MinValue;
            MyQueue<BinaryTree.Node<int>> queue = new MyQueue<BinaryTree.Node<int>>(tree.Root);

            // Traverse the tree via breadth-first traversal
            while (queue.Length > 0)
            {
                BinaryTree.Node<int> node = queue.Dequeue();

                // If the current node's value is higher than the previously highest found
                // value, then overwrite it with this node's value
                if (node.Value > maximumValue)
                {
                    maximumValue = node.Value;
                }

                if (node.Left != null)
                {
                    queue.Enqueue(node.Left);
                }
                if (node.Right != null)
                {
                    queue.Enqueue(node.Right);
                }
            }

            return maximumValue;
        }
    }
}