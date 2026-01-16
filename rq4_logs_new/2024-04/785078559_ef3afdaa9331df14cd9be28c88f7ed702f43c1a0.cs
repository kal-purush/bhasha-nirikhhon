using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace stack
{
    class Program
    {
        public class Stack
        {
            //propertys
            int size;
            int[] arr;
            int top;
            //Cunstructur
             public Stack(int size)
            {
                this.size = size;
                this.arr = new int[size];
                this.top = -1;
                
            }
            public void push(int value)
            {
                //if have space than do this
                if (size - top > 1)
                {
                    top++;
                    arr[top] = value;
                }
                else
                {
                    // have NOT Space
                    Console.WriteLine("stack is overflow");
                }

            }
            public int pop()
            {
                if (top >= 0)
                {
                    int ans = arr[top];
                    top--;
                    return ans;
                }
                else
                {
                    Console.WriteLine("stack is  underflow");
                    return -1;
                }

            }
            public int peek()
            {
                if (isEmpty())
                {
                    Console.WriteLine("stack is empty");
                    return -1;
                }
                else
                {
                    //Console.WriteLine(arr[top]);
                    return arr[top];

                }


            }
            public bool isEmpty()
            {
                if (top >= 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }

            }

        }
        static void Main(string[] args)
        {
            //MY stack ------------------------------------------------------------------


            Stack s = new Stack(5);
             s.push(23);
             s.push(24);
             s.push(25);
             s.push(26);
             Console.WriteLine("pop" +s.pop());
             Console.WriteLine( "peek"+s.peek());
             Console.WriteLine("pop" + s.pop());
             Console.WriteLine("peek" + s.peek());
             Console.WriteLine("pop" + s.pop());
            
             Console.WriteLine("peek" + s.peek());
             Console.WriteLine("pop" + s.pop());
            Console.WriteLine("peek" + s.peek());
            //Console.WriteLine(s.peek());
            Console.WriteLine(s.isEmpty());
            








            //stack using generics------------------------------------------------------------
            //Stack<int> stack  = new Stack<int>();
            //stack.Push(0);
            //stack.Push(1);
            //stack.Push(2);
            //stack.Push(3);
            //stack.Push(4);
            //stack.Push(45);
            //foreach(int i in stack) {
            //    Console.WriteLine(i);

            //}
            //Console.WriteLine(stack.Peek());
            //Console.WriteLine(stack.Pop());
            //Console.WriteLine(stack.Peek());
            //--------------------------------------------------------------------------------------------




        }
    }
}