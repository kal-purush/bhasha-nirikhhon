using KAryTree;
using StackAndQueue;
using System;

namespace FindMatches
{
    class Program
    {
        public static void Main(string[] args)
        {

            KAryTree<char> tree = new KAryTree<char>('A');

            #region Build up the tree with duplicate nodes (2 A, 2 B, 2 C, 3 D)
            tree.Add('A', 'A');
            tree.Add('B', 'A');
            tree.Add('C', 'A');
            tree.Add('B', 'B');
            tree.Add('C', 'B');
            tree.Add('D', 'C');
            tree.Add('D', 'D');
            tree.Add('D', 'B');
            #endregion

            MyQueue<KAryNode<char>> matchesA = FindMatches(tree, 'A');
            MyQueue<KAryNode<char>> matchesB = FindMatches(tree, 'B');
            MyQueue<KAryNode<char>> matchesC = FindMatches(tree, 'C');
            MyQueue<KAryNode<char>> matchesD = FindMatches(tree, 'D');

            Console.WriteLine("Finding all matches for a k-ary tree with duplicate values.");
            Console.WriteLine();
            Console.Write("Expecting 2 A's, found: ");

            while (matchesA.Length > 0)
            {
                Console.Write($"{matchesA.Dequeue().Value} ");
            }

            Console.WriteLine();
            Console.Write("Expecting 2 B's, found: ");

            while (matchesB.Length > 0)
            {
                Console.Write($"{matchesB.Dequeue().Value} ");
            }

            Console.WriteLine();
            Console.Write("Expecting 2 C's, found: ");

            while (matchesC.Length > 0)
            {
                Console.Write($"{matchesC.Dequeue().Value} ");
            }

            Console.WriteLine();
            Console.Write("Expecting 3 D's, found: ");

            while (matchesD.Length > 0)
            {
                Console.Write($"{matchesD.Dequeue().Value} ");
            }

            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine("Please press any key to exit this demo...");
            Console.ReadKey();
        }

        /// <summary>
        /// For the provided k-ary tree, return all nodes containing the provided searchValue
        /// to the caller in a collection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tree"></param>
        /// <param name="searchValue"></param>
        /// <returns></returns>
        public static MyQueue<KAryNode<T>> FindMatches<T>(KAryTree<T> tree, T searchValue)
        {
            MyQueue<KAryNode<T>> matches = new MyQueue<KAryNode<T>>();
            MyQueue<KAryNode<T>> nodes = new MyQueue<KAryNode<T>>(tree.Root);

            while (nodes.Length > 0)
            {
                KAryNode<T> currentNode = nodes.Dequeue();

                if (currentNode.Value.Equals(searchValue))
                {
                    matches.Enqueue(currentNode);
                }

                foreach (KAryNode<T> node in currentNode.Children)
                {
                    nodes.Enqueue(node);
                }
            }

            return matches;
        }
    }
}