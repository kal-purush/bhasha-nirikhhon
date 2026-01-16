using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinarySearchTrees
{
    class BinaryTree
    {
        public Node left;
        public Node right;
        public void Insert(int data)
        {
            Node newnode = new Node(data);

            // Pointer to start traversing from root andtraverses downward path to search where the new node to be inserted
            Node x = null;

            // Pointer y maintains the trailing pointer of x
            Node y = null;

            while (x != null)
            {
                y = x;
                if (data < x.data)
                    x = x.left;
                else
                    x = x.right;
            }

            // If the root is null i.e the tree is empty the new node is the root node
            if (y == null)
                y = newnode;

            // If the new key is less then the left node data Assign the new node to be its left child
            else if (data < y.data)
                y.left = newnode;

            // else assign the new node its right child
            else
                y.right = newnode;

            // Returns the pointer where the new node is inserted
            Console.WriteLine(y.data);
        }
    }
}