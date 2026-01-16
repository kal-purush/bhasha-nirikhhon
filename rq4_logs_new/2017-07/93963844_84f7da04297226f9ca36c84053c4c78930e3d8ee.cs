using System;
using System.Collections;

namespace CollectionsProject
{
    class CollectionsDemo
    {
        public static void Main()
        {
            Stack _stack = new Stack();
            _stack.Push(1);
            _stack.Push("<script>virus...</script>");
            _stack.Push('$');
            _stack.Push(true);
            _stack.Push(1.2);            

            foreach (object element in _stack)
            {
                Console.WriteLine(element);
            }

            _stack.Pop();
            _stack.Pop();

            Console.WriteLine("\n\nAfter Pop()");
            foreach (object element in _stack)
            {
                Console.WriteLine(element);
            }

            Console.ReadLine(); 
        }
    }
}