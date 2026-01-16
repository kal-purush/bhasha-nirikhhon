using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace BinaryTree
{
    internal class Program
    {
        public class Node
        {
            public  int data;
            public Node left;
            public Node right;
           public  Node(int d)
            {
                this.data = d;
                this.left = null;
                this.right = null;
            }
        }
        static Node Buildtree(ref Node root)
        {
            Console.WriteLine("Enter the data root node :");
            int data = Convert.ToInt32(Console.ReadLine());
            //base case
            if(data == -1)
            {
                return null;
            }
            //create new node 
            root = new Node(data);
            //recursive call for left
            Console.WriteLine($"Enter the data root node for left {data} :");
            root.left = Buildtree(ref root.left);
            //recursive call for right node
            Console.WriteLine($"Enter the data root node for right {data} :");
            
            root.right = Buildtree(ref root.right);

            return root;

        }
        //preorder
        static void printNode( Node root)
        {
           if(root== null)
            {
                return;
            }
            Console.Write(root.data);
            printNode(root.left);
            printNode(root.right);
            
        }
        // level order traversal
        //void levelOrderTravarsal()
        //{

        //}
        static void Main(string[] args)
        {
            Node root =null ;
            //build tree

            Buildtree(ref root);
            printNode( root);


        }
    }
}