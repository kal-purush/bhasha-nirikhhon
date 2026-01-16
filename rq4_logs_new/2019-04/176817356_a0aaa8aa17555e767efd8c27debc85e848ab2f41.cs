using System;
using System.Collections.Generic;
using System.Text;

namespace BinaryTree.Classes
{
    public class BST : BT
    {
        /// <summary>
        /// Add a node to a BST
        /// </summary>
        /// <param name="value">Node to add</param>
        public void Add(int value)
        {
            Node node = new Node();
            node.Value = value;
            Node current = Root;


            if (current == null)
            {
                Root = node;
            }

            while (current != null)
            {
                if (value >= current.Value)
                {
                    if (current.Right != null)
                    {
                        current = current.Right;
                    }
                    else
                    {
                        current.Right = node;
                        break;
                    }
                }
                else
                {
                    if (current.Left != null)
                    {
                        current = current.Left;
                    }
                    else
                    {
                        current.Left = node;
                        break;
                    }
                }
                    
            }
        }

        /// <summary>
        /// See if a BST contains a node
        /// </summary>
        /// <param name="value">Int to look for</param>
        /// <returns>Does contain</returns>
        public bool Contains(int value)
        {
            int[] list = PreOrder();
            foreach (int num in list)
            {
                if (num == value)
                {
                    return true;
                }
            }
            return false;
        }
    }
}