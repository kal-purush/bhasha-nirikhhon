using System;
using System.Collections.Generic;
using System.Text;

namespace Lesson_2.Library
{
    public class BidirectionalList : ILinkedList
    {
        private int _count;
        public Node First { get; private set; }
        public Node Last { get; private set; }

        public void AddNode(int value)
        {
            Node node = new Node(this, value);

            if (First == null)
            {
                Last = First = node;
                _count++;
            }
            else
                AddNodeAfter(Last, value);
        }

        public void AddNodeAfter(Node node, int value)
        {

            if (node == null)
            {
                AddNode(value);
                return;
            }
            
            CheckNode(node);

            var nextNode = node.NextNode;
            Node newNode = new Node(this, value)
            {
                PrevNode = node,
                NextNode = nextNode
            };

            node.NextNode = newNode;

            if (nextNode != null)
                nextNode.PrevNode = newNode;

            if (node == Last)
                Last = newNode;
            _count++;
        }

        public Node FindNode(int searcheValue)
        {
            var node = First;
            while (node != null)
            {
                if (node.Value == searcheValue)
                    return node;
                node = node.NextNode;
            }
            return null;
        }

        public int GetCount()
        {
            return _count;
        }

        public void RemoveNode(int index)
        {
            var node = First;
            int counter = 0;
            while (node != null)
            {
                if (counter == index)
                {
                    RemoveNode(node);
                }
                node = node.NextNode;
                counter++;
            }
        }

        public void RemoveNode(Node node)
        {
            CheckNode(node);

            if (_count == 1)
                First = Last = null;
            else
            {
                if (node.PrevNode == null)
                {
                    First = node.NextNode;
                    node.NextNode.PrevNode = null;
                }
                else
                    node.PrevNode.NextNode = node.NextNode;

                if (node.NextNode == null)
                {
                    node.PrevNode.NextNode = null;
                    Last = node.PrevNode;
                }
                else
                    node.NextNode.PrevNode = node.PrevNode;
            }
            _count--;
        }

        private void CheckNode(Node node)
        {
            if (node == null)
                throw new ArgumentNullException("node");

            if (node.list != this)
                throw new Exception("Node does not belong to the current list");
        }

        public override string ToString()
        {
            string nullString = "NULL";
            StringBuilder sb = new StringBuilder();
            var node = First;
            int counter = 0;
            sb.AppendLine($"Count: {GetCount()}");

            while (node != null)
            {
                if (node == First)
                    sb.AppendLine($"First node: idx({counter}) {node.Value.ToString().PadLeft(4)}");

                sb.AppendLine($"({counter}) {(node.PrevNode == null ? nullString : node.PrevNode.Value.ToString().PadLeft(4))} <- {node.Value.ToString().PadLeft(4)} -> {(node.NextNode == null ? nullString : node.NextNode.Value.ToString().PadLeft(4))}");
                if (node == Last)
                    sb.AppendLine($"Last node: idx({counter}) {node.Value.ToString().PadLeft(4)}");
                node = node.NextNode;
                counter++;
            }

            return sb.ToString();
        }

    }
}