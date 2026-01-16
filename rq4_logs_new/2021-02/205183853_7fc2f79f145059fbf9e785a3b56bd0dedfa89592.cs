using System;
using System.Collections.Generic;

namespace Linear_DataStructures.Queue
{
    public class QueueQuestions
    {
        
        public static void Reverse () 
        {
            Queue<int> ql=new Queue<int>();
            ql.Enqueue(10);
            ql.Enqueue(20);
            ql.Enqueue(30);

            Stack<int> s= new Stack<int>();

            while(ql.Count>0)
            {
                s.Push(ql.Dequeue());
                
            }
            
            while(s.Count>0)
            {
                Console.WriteLine(s.Peek().ToString());
                ql.Enqueue(s.Pop());
            }
            
        }

        public static void DriverCode()
        {
            Reverse();
        }
        
    }
}