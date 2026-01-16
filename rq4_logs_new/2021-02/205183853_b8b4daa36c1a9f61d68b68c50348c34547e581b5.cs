using System.Collections.Generic;
using System;
namespace Linear_DataStructures
{
    public class StackQueue
    {
        System.Collections.Generic.Stack<int> s1 = new Stack<int>();
        System.Collections.Generic.Stack<int> s2 = new Stack<int>();

        public void Enqueue(int item)
        {
            s1.Push(item);
        }

        public int Dequeue()
        {
            if (Empty())
            {
                throw new InvalidOperationException("queue empty");
            }
            else
            {
                MoveStack1toStack2();
                return s2.Pop();
            }
        }
        public int Peek()
        {
            if (Empty())
            {
                throw new InvalidOperationException("queue empty");
            }
            else
            {
               MoveStack1toStack2();
                return s2.Peek();
            }
        }
        public  bool Empty()
        {
            return s1.Count == 0 && s2.Count == 0;
        }

        public void MoveStack1toStack2()
        {
             if (s2.Count == 0)
                {
                    int count=s1.Count;
                    for (int i = 0; i < count; i++)
                    {
                        int value = s1.Pop();
                        s2.Push(value);
                    }
                }
        }
        public static void DriverCode()
        {
             StackQueue stackQueue= new StackQueue();
             stackQueue.Enqueue(10);
             stackQueue.Enqueue(20);
             stackQueue.Enqueue(30);
             System.Console.WriteLine(stackQueue.Dequeue());
             System.Console.WriteLine(stackQueue.Dequeue());
             System.Console.WriteLine(stackQueue.Dequeue());
            // System.Console.WriteLine(stackQueue.Dequeue());
             stackQueue.Enqueue(50);
             stackQueue.Enqueue(60);
             stackQueue.Enqueue(30);
              System.Console.WriteLine(stackQueue.Peek());
             System.Console.WriteLine(stackQueue.Dequeue());
            System.Console.WriteLine(stackQueue.Peek());
        }


    }
}