using System;
using System.Text;

namespace StackAndQueue
{
    class MyQueue<T>
    {
        internal Node<T> Front { get; private set; }
        internal Node<T> Back { get; private set; }

        public int Length { get; private set; }

        public MyQueue(T frontValue)
        {
            Front = new Node<T>(frontValue);
            Back = Front;
            Length = 1;
        }

        public MyQueue()
        {
            Front = null;
            Back = Front;
            Length = 0;
        }

        public void Enqueue(T newValue)
        {
            /*
            // If the queue is empty, set Front to a new Node object
            // containing the passed value and set Back to Front
            if (Front is null)
            {
                Front = new Node<T>(newValue);
                Back = Front;
            }
            else
            {
                // Set back to a new Node object containing the passed
                // value and has a Next reference to the old Back object's
                // next Node reference or to Front if Back was equal to
                // Front (when Length == 1)
                Back = new Node<T>(newValue, Back.Next ?? Front);
            }
            */

            // 
            if (Front is null)
            {
                Front = new Node<T>(newValue);
                Front.Next = Back = null;
            }
            else
            {
                Back.Next = new Node<T>(newValue);
                Back = Back.Next;
            }

            Length++;
        }

        public T Dequeue()
        {

        }
    }
}