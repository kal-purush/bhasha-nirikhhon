using System;
using System.Collections.Generic;
using System.Text;

namespace LinkedList.Classes
{
    class LList
    {
        public Node Head { get; set; }

        public LList(int value)
        {
            Node node = new Node(value);
        }

        /// <summary>
        /// creates a new node and adds that node to the head 
        /// </summary>
        /// <param name="value"></param>
        public void Insert(int value)
        {//need to check that everythong works
            
            Node node = new Node(value);
            node.Next = Head;
            Head = node;
        }
    }
}