using System;
using FizzBuzzTree.Classes;

namespace FizzBuzzTree
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to Fizz Buzz Tree!");

            BinaryTree bt = new BinaryTree();
            Node three = new Node(3);
            Node one = new Node(1);
            Node two = new Node(2);
            Node five = new Node(5);
            Node fifteen = new Node(15);
            Node six = new Node(6);
            Node ten = new Node(10);

            bt.Root = one;
            one.Left = three;
            one.Right = five;
            three.Left = fifteen;
            three.Right = six;
            five.Left = ten;
            five.Right = two;

            FizzBuzzTree(bt);
            Console.WriteLine(bt.Root.Left.Value);
            Console.Read();
        }
        
        public static BinaryTree FizzBuzzTree(BinaryTree bt)
        {
            if(bt.Root != null)
                InOrder(bt.Root);
            return bt;
        }
        private static void InOrder(Node root)
        {
            if (root.Left != null)
                InOrder(root.Left);
            FizzBuzzCheck(root);
            if (root.Right != null)
                InOrder(root.Right);
        }
        public static Node FizzBuzzCheck(Node root)
        {
            if (Convert.ToInt32(root.Value) % 5 == 0 && Convert.ToInt32(root.Value) % 3 == 0)
                root.Value = "FizzBuzz";
            else if(Convert.ToInt32(root.Value) % 5 == 0)
                root.Value = "Buzz";
            else if (Convert.ToInt32(root.Value) % 3 == 0)
                root.Value = "Fizz";
            return root;
        }
    }
}