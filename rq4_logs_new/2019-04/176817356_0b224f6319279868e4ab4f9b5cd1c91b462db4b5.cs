using System;
using System.Collections.Generic;
using System.Text;

namespace StackAndQueue.Classes
{
    public class Queue
    {
        public Node Back;

        /// <summary>
        /// Enwueue onto a Queue
        /// </summary>
        /// <param name="value">Number to Enqueue</param>
        public void Enqueue(int value)
        {
            Node newNode = new Node();
            newNode.Value = value;
            newNode.Next = Back;
            Back = newNode;
        }

        /// <summary>
        /// Dequeue from front of Queue
        /// </summary>
        /// <returns>Dequeued Node</returns>
        public Node Dequeue()
        {
            if (Back == null)
            {
                return null;
            }

            Node current = Back;

            if (current.Next == null)
            {
                Back = null;
                return current;
            }

            while (current.Next.Next != null)
            {
                current = current.Next;
            }

            Node temp = current.Next;
            current.Next = null;
            return current;
        }

        /// <summary>
        /// Look at node to be dequeued
        /// </summary>
        /// <returns>Node in front</returns>
        public Node Peek()
        {
            if (Back == null)
            {
                return null;
            }

            Node current = Back;
            while (current.Next != null)
            {
                current = current.Next;
            }
            return current;
        }
    }
}