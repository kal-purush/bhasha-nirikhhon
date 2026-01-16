using System;
using System.Collections.Generic;
using System.Text;

namespace LinkedList
{
    public class LinkedListClass
    {
        public NodeClass Head { get; set; }

        public LinkedListClass(NodeClass node)
        {
            Head = node;
        }

        public void AddFirst(NodeClass newNode)
        {
            newNode.Next = Head;
            Head = newNode;
        }

        public void AddLast(NodeClass newNode)
        {
            NodeClass Current = Head;
            while(Current.Next != null)
            {
                Current = Current.Next;
            }
            Current.Next = newNode;
        }

        public void AddBefore(NodeClass targetNode, NodeClass newNode)
        {
            NodeClass Current = Head;
            while(Current.Next != targetNode)
            {
                Current = Current.Next;
            }
            newNode.Next = Current.Next;
            Current.Next = newNode;
        }

        public void AddAfter(NodeClass targetNode, NodeClass newNode)
        {
            NodeClass Current = Head;
            while(Current != targetNode)
            {
                Current = Current.Next;
            }
            newNode.Next = targetNode.Next;
            targetNode.Next = newNode;
        }
    }
}