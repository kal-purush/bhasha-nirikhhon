using System.Collections.Generic;

namespace Linear_DataStructures
{
    public class ReverseQueue
    {
        public void Reverse(Queue<int> items, int k ){
            Stack<int> s= new Stack<int>();
            
            int count=0;
            // Dequeue the first K elements from the queue
            // and push them onto the stack
            while(count<k)
            {
                s.Push(items.Dequeue());
                count++;
            }
            // Enqueue the content of the stack at the
            // back of the queue
            while(s.Count>0)
            {
                items.Enqueue(s.Pop());
            }
            // Add the remaining items in the queue (items
           // after the first K elements) to the back of the
           // queue and remove them from the beginning of the queue
            for(int i=0;i<items.Count-k;i++)
            {
                int item = items.Dequeue();
                items.Enqueue(item);
            }


        }

        public static void DriverCode(){
            ReverseQueue reverse= new ReverseQueue();
            Queue<int> a= new Queue<int>();
            a.Enqueue(20);
            a.Enqueue(30);
            a.Enqueue(10);
            a.Enqueue(50);
            a.Enqueue(40);
            reverse.Reverse(a,3);

        }
    }
}