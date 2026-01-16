using BinaryTree;
using System;
using System.Collections.Generic;

namespace FizzBuzzTree
{
    public class Program
    {
        static void Main(string[] args)
        {
            // Enumerates three test cases of trees. Based on the xUnit project's
            // test tree data
            TreeTestData testData = new TreeTestData();

            foreach (Tree<string> tree in testData)
            {
                Console.WriteLine("In order traversal of source tree:");
                List<string> list = new List<string>();
                list = tree.InOrderTraversal(tree.Root, list);
                foreach (string item in list)
                {
                    Console.WriteLine(item);
                }

                Console.WriteLine("\nIn order traversal of fizz buzzed tree:");
                list.Clear();
                Tree<string> fbTree = FizzBuzzTree(tree);
                list = fbTree.InOrderTraversal(fbTree.Root, list);

                foreach (string item in list)
                {
                    Console.WriteLine(item);
                }

                Console.WriteLine("\nPress any key for next demo...");
                Console.ReadKey(true);
                Console.Clear();
            }

            Console.WriteLine("Please press any key to exit this demo...");
            Console.ReadKey(true);
        }

        /// <summary>
        /// Helper method needed to fullfill the method signature of the
        /// FizzBuzzTree method per assignment guidelines.
        /// </summary>
        /// <param name="node">The current node to operate on</param>
        internal static void FizzBuzzRecursion(Node<string> node)
        {
            if (node.Left != null)
            {
                FizzBuzzRecursion(node.Left);
            }

            if (node.Right != null)
            {
                FizzBuzzRecursion(node.Right);
            }

            int nodeValue = Convert.ToInt32(node.Value);

            // Check for fizzes or buzzes or fizzbuzzes
            if (nodeValue % 3 == 0 && nodeValue % 5 == 0)
            {
                node.Value = "FizzBuzz";
            }
            else
            {
                if (nodeValue % 3 == 0)
                {
                    node.Value = "Fizz";
                }
                if (nodeValue % 5 == 0)
                {
                    node.Value = "Buzz";
                }
            }
        }

        /// <summary>
        /// Performs in-order traversal of the provided tree of strings that can
        /// be converted to System.Int32, checking the converted values against
        /// FizzBuzz rules.
        /// </summary>
        /// <param name="tree">The source binary tree containing strings that can
        /// be converted to int</param>
        /// <returns>The transformed tree containing strings representing whether
        /// the source nodes are Fizz, Buzz, or FizzBuzz</returns>
        public static Tree<string> FizzBuzzTree(Tree<string> tree)
        {
            FizzBuzzRecursion(tree.Root);
            return tree;
        }
    }
}