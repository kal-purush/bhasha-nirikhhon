using System;
using System.Collections.Generic;
using System.Linq;

public class MyStack
{
    private List<int> stack = new List<int>();

    public void Push(int num)
    {
        stack.Add(num);
    }

    public void Pop()
    {
        if (stack.Count == 0)
        {
            throw new IndexOutOfRangeException();
        }
        else {
            stack.RemoveAt(stack.Count - 1);
        }
    }

    public int Peek()
    {
        if (stack.Count == 0)
        {
            throw new IndexOutOfRangeException();
        }
        else
        {
            return stack[stack.Count - 1];
        }
    }
}

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            MyStack myStack = new MyStack();
            myStack.Push(10);
            myStack.Push(11);
            myStack.Push(12);
            myStack.Push(13);

            Console.WriteLine("peek" + myStack.Peek());
            ///pop
            myStack.Pop();
            Console.WriteLine("new peek" + myStack.Peek());
        }
    }
}