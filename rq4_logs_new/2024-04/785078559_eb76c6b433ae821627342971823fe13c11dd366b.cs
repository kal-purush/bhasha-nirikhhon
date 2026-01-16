using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Circular_Queue
{
    internal class Program
    {
        class C_Queue
        {
            //propertis
            int size;
            int[] arr;
            int front;
            int rarer; 

            public C_Queue(int size)
            {
                this.size = size;
                this.arr = new int[size];
                this.front = -1;
                this.rarer = -1;
            }

            public bool Enqueue(int value)
            {
                if ((front == 0 && rarer == size-1) || (rarer ==front-1)) //to check is empty
                {
                    Console.WriteLine("Queue is full");
                    return false;
                }
                else if (front == -1)// first element push 
                {
                    front = rarer = 0;
                    arr[rarer] = value;

                }
                else if (rarer == size - 1 && front != 0) //maintain cyclic nature
                {
                    rarer = 0; 
                    arr[rarer] = value;
                    
                    
                }
                else //normal flow
                {
                    rarer++;
                    arr[rarer] = value;
                    
                }
                return true;
               
                

            }
            public int Dequeue()
            {
                if(front == -1) //to check empty
                {
                    Console.WriteLine("Queue is empty");
                    return -1;
                }
                int ans = arr[front];
                if(front == rarer)  //single element is present
                {
                    front =rarer = -1;
                }
                else if(front ==size-1)
                {
                    front = 0;
                }
                else
                {
                    front++;
                }
                return ans;


            }

            public bool isEmpty()
            {
                if ((front == 0 && rarer == size - 1) || (rarer == front - 1))
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
            public int peek()
            {
                if (front ==-1)
                {
                    return -1;
                }
                else
                {
                    return arr[front];
                }
            }





        }
        static void Main(string[] args)
        {
            C_Queue c = new C_Queue(5);
            c.Enqueue(1);
            c.Enqueue(2);
            c.Enqueue(4);
            c.Enqueue(7);
            c.Enqueue(8);
            Console.WriteLine(c.isEmpty());


            Console.WriteLine("thr peeak "+ c.peek());
            Console.WriteLine("thr dequeue is "+ c.Dequeue());
            Console.WriteLine("thr peeak " + c.peek());

            Console.WriteLine("thr dequeue is "+ c.Dequeue());
            Console.WriteLine("thr peeak " + c.peek());

            Console.WriteLine("thr dequeue is "+ c.Dequeue());
            Console.WriteLine("thr peeak " + c.peek());

            Console.WriteLine("thr dequeue is "+ c.Dequeue());
            Console.WriteLine("thr peeak " + c.peek());

            Console.WriteLine(c.isEmpty());

        }
    }
}