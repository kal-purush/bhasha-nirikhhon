using System;

namespace Linear_DataStructures
{
    public class ArrayQueue//implementation of circlular queue using array
    {
        int[] a;
        int length;
        int count=0;
        int frontpointer;

        int rearpointer;
          ArrayQueue(int n)
        {
            length=n;
            a= new int[length];
            frontpointer =count;
            rearpointer=count;

        }

        public void Enqueue(int value)
        {
            if(IsFull())
            {
                throw new InvalidOperationException("array is full");
            }
          
            a[rearpointer]=value;
            rearpointer=(rearpointer+1)%length;
            count++;
        }

        public int Dequeue()
        {
            if(IsEmpty())
            {
                throw new InvalidOperationException("array is empty");
            }
            int item =a[frontpointer];
            
            a[frontpointer]=0;
            frontpointer=(frontpointer+1)%length;
            count--;
            System.Console.WriteLine(item);
            return item;
        }

        public int Peek(){
            if(IsEmpty())
            {
                throw new InvalidOperationException("array is empty");
            }
            return a[frontpointer];
        }

        public bool IsEmpty()
        {
           return count==0;
        }

        public bool IsFull()
        {
            return count==length;
        }
        public void DisplayElements(){
            for(int i=0;i<length;i++)
            {
                Console.Write(a[i]+" ");
            }
            Console.WriteLine();
        }

        public static void DriverCode(){
            ArrayQueue arrayQueue= new ArrayQueue(4);
            arrayQueue.Enqueue(10);
            arrayQueue.Enqueue(20);
            arrayQueue.Enqueue(30);
            arrayQueue.Enqueue(40);
            arrayQueue.Dequeue();
            arrayQueue.Enqueue(50);
            arrayQueue.Dequeue();
            arrayQueue.Dequeue();
            arrayQueue.Dequeue();
            arrayQueue.Enqueue(60);
            arrayQueue.Enqueue(70);
            arrayQueue.Enqueue(90);
    
            arrayQueue.DisplayElements();
            arrayQueue.Dequeue();
            arrayQueue.Enqueue(80);
             arrayQueue.DisplayElements();
            Console.WriteLine();

        }
    }
}