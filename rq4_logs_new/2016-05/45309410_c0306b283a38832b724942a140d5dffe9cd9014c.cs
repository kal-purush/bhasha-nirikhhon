using System;

namespace StackNamespace
{
    /// <summary>
    /// LIFO structure based on the array
    /// </summary>
    public class StackArray : IStack
    {
        public void Push(int value)
        {
            if (position == size - 1)
            {
                throw new StackOverflowException("Stack is full");
            }

            ++position;
            array[position] = value;
        }

        public int Top()
        {
            if (position == -1)
            {
                throw new EmptyStackException("you are trying to access a non-existent object");
            }

            return array[position];
        }

        public void Pop()
        {
            if (position == -1)
            {
                throw new EmptyStackException("Stack is empty");
            }

            --position;
        }

        public bool IsEmpty() => position == -1;

        public void Clear()
        {
            position = -1;
        }

        public void Print()
        {
            if (position == -1)
            {
                Console.WriteLine("Stack is empty");
                return;
            }

            foreach (int element in array)
            {
                Console.WriteLine(element + " ");
            }
        }

        private int position = -1;
        private const int size = 100;
        private int[] array = new int[size];
    }
}