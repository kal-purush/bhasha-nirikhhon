using System;
using System.Text;
using KAryTree;
using StackAndQueue;

namespace PrintLevelOrder
{
    class Program
    {
        static void Main(string[] args)
        {
            KAryTree<char> tree = new KAryTree<char>('A');

            #region Seed Tree With Example Values From Instructions
            tree.Add('B', 'A');
            tree.Add('C', 'A');
            tree.Add('D', 'A');
            tree.Add('E', 'A');
            tree.Add('F', 'A');
            tree.Add('G', 'A');

            tree.Add('H', 'B');
            tree.Add('I', 'B');
            tree.Add('J', 'B');

            tree.Add('K', 'E');
            tree.Add('L', 'E');

            tree.Add('M', 'G');

            tree.Add('N', 'H');
            tree.Add('O', 'H');

            tree.Add('P', 'K');

            tree.Add('Q', 'L');
            #endregion

            string output = PrintLevelOrder(tree);

            Console.WriteLine(output);

            Console.WriteLine("Please press any key to exit this demo...");
            Console.ReadKey();
        }

        /// <summary>
        /// Returns a string representation of all nodes in the provided tree
        /// with newline delimiters between rows in the tree
        /// </summary>
        /// <typeparam name="T">The type of values held in the tree</typeparam>
        /// <param name="tree">The tree to produce a level order string for</param>
        /// <returns>A string representation of all node values from the provided
        /// tree delimited by newline characters between the tree's rows</returns>
        public static string PrintLevelOrder<T>(KAryTree<T> tree)
        {
            StringBuilder sb = new StringBuilder();
            MyQueue<KAryNode<T>> nodeQueue = new MyQueue<KAryNode<T>>(tree.Root);

            // Holds all values from the nodes of the current row
            MyQueue<T> rowValues = new MyQueue<T>();
            // The number of items in the current row of nodes
            int rowColumns = 1;

            // Breadth-first Traversal of tree
            while (nodeQueue.Length > 0)
            {
                KAryNode<T> currentNode = nodeQueue.Dequeue();
                // Store the currentNode's value in the queue of values for the current row
                rowValues.Enqueue(currentNode.Value);

                foreach (KAryNode<T> child in currentNode.Children)
                {
                    nodeQueue.Enqueue(child);
                }

                // If we have reached the end of a row of values, append all the values that
                // have been enqueued to the rowValues queue to the string builder
                if (rowValues.Length >= rowColumns)
                {
                    while (rowValues.Length > 0)
                    {
                        sb.Append($"{rowValues.Dequeue()} ");
                    }

                    // Add a newline to the string builder to signal the end of this row
                    sb.AppendLine();

                    // The next row of values are all in the nodeQueue at this point in iteration.
                    // Store that queue's length to know when we've reached the end of the next row.
                    rowColumns = nodeQueue.Length;
                }
            }

            return sb.ToString();
        }
    }
}