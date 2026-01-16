using System;
namespace Linear_DataStructures
{
    public class LinkedListQueue
    {
        public class Node
        {
            internal int value;

            internal Node next;
            public Node(int value)
            {
                this.value = value;
            }
        }

        private Node first;

        private Node last;

        private int size;
        //O(1)
        public void Enqueue(int value)
        {
            var node = new Node(value);
            if (IsEmpty())
            {
                first = last = node;
            }
            else
            {
                last.next = node;
                last = node;
            }

            size++;
        }

        //O(1)
        public int Dequeue()
        {
            if (IsEmpty())
            {
                throw new InvalidOperationException();
            }
            int value;
            if (first == last)
            {
                value = first.value;
                first = last = null;
            }
            else
            {
                value = first.value;
                var node = first.next;
                first = node;
            }
            size--;
            return value;
        }
        //O(1)
        public int peek()
        {
            if (IsEmpty())
            {
                throw new InvalidOperationException();
            }

            return first.value;
        }
        //O(1)
        public bool IsEmpty()
        {
            return size == 0;
        }

        public static void DriverCode()
        {
            LinkedListQueue linkedListQueue = new LinkedListQueue();
            linkedListQueue.Enqueue(10);
            linkedListQueue.Enqueue(20);
            linkedListQueue.Enqueue(30);
            linkedListQueue.Enqueue(40);
            System.Console.WriteLine(linkedListQueue.Dequeue());
            System.Console.WriteLine(linkedListQueue.Dequeue());
            System.Console.WriteLine(linkedListQueue.peek());
            System.Console.WriteLine(linkedListQueue.size);
            linkedListQueue.Enqueue(50);
            System.Console.WriteLine(linkedListQueue.size);
        }
    }
}