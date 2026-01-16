using System;
using BreadthFirst.Classes;
using System.Collections;

namespace BreadthFirst
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to Bread Traversal, where we slice it up!");

            BinaryTree bt = new BinaryTree();
            Node one = new Node(1);
            Node two = new Node(2);
            Node three = new Node(3);
            Node four = new Node(4);
            Node five = new Node(5);
            Node six = new Node(6);
            Node seven = new Node(7);

            bt.Root = one;
            one.Left = two;
            one.Right = three;
            two.Left = four;
            two.Right = five;
            three.Left = six;
            three.Right = seven;

            BreadthFirst(bt);
            Console.Read();
        }
        public static string BreadthFirst(BinaryTree bt)
        {
            string solution = "";
            Queue que = new Queue();
            if (bt.Root != null)
            {
                Console.WriteLine(bt.Root.Value);
                solution += $"{ bt.Root.Value } ";
                if (bt.Root.Left != null)
                    que.Enqueue(bt.Root.Left);
                if (bt.Root.Right != null)
                    que.Enqueue(bt.Root.Right);

                while (que.Count != 0)
                {
                    // Sean - When using the System.Collections.Queue
                    // we have to cast the object returned by the Dequeue method
                    // into the Node object, this gives access to Left and Right props

                    Object obj = que.Dequeue();
                    Node node = obj as Node;

                    // Sean - Action on Node

                    Console.WriteLine(node.Value);
                    solution += $"{ node.Value } ";


                    // Sean - Continue traversal of Tree if children Nodes exist

                    if (node.Left != null)
                        que.Enqueue(node.Left);
                    if (node.Right != null)
                        que.Enqueue(node.Right);
                }
            }
            return solution;
        }
    }
}