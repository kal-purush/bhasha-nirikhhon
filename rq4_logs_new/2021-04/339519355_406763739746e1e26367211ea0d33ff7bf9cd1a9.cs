using System;
using System.Threading;
using System.Threading.Tasks;
using static System.Console;
using System.Collections.Generic;
using Library;

namespace Homework
{
    class Program
    {
        static void Main(string[] args)
        {
            WindowWidth = 100;
            WindowHeight = 24;
            ResetColor();
            Clear();

            int[] values = { 16, 8, 4, 2, 1, 3, 6, 5, 7, 12, 10, 9, 11, 14, 13, 15, 24, 20, 18, 17, 19, 22, 21, 23, 28, 26, 25, 27, 30, 29, 31 };
            BinnaryTree tree = new BinnaryTree();

            foreach (var item in values)
            {
                tree.AddItem(item);
            }

            var printInfo = tree.PrintTree();
            var nodes = tree.BreadthFirstSearch();
            nodes.AddRange(tree.DeepFirstSearch());

            Write("\nPress any key to exit");
            CursorVisible = false;

            PrintNodesAsync(nodes, printInfo);

            ReadKey();
        }

        static void PrintNode(string text, Point position, ConsoleColor color)
        {
            SetCursorPosition(position.X, position.Y);
            ForegroundColor = color;
            Write($"({text})");
        }

        const int SPEED = 300;
        static async Task PrintNodesAsync(List<TreeNode> nodes, Dictionary<TreeNode, Point> printInfo)
        {
            await Task.Run(() =>
            {
                int i = 0;
                int half = nodes.Count / 2;
                while (true)
                {
                    i = i % nodes.Count;
                    var color = i < nodes.Count / 2 ? ConsoleColor.Green : ConsoleColor.Red;
                    var previousColor = i <= (nodes.Count / 2) ? ConsoleColor.DarkGreen : ConsoleColor.DarkRed;
                    if (i > 0)
                        PrintNode(nodes[i - 1].Value.ToString(), printInfo[nodes[i - 1]], previousColor);
                    PrintNode(nodes[i].Value.ToString(), printInfo[nodes[i]], color);
                    Thread.Sleep(SPEED);
                    i++;
                }
            });
        }
    }
}