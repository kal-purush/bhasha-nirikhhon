using System;
using StackAndQueue;

namespace QueueWithStacks
{
    public class Program
    {
        static void Main(string[] args)
        {
            MyStack<int> inStack = new MyStack<int>();
            MyStack<int> outStack = new MyStack<int>();


            Console.ReadKey();
        }

        /// <summary>
        /// Enqueues onto a "fake" queue making use of two MyStack objects of
        /// type <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T">The type of values held in the provided stacks</typeparam>
        /// <param name="inStack">The stack representing incoming values</param>
        /// <param name="outStack">The stack representing outgoing values</param>
        /// <param name="newValue">The value that the newly enqueued Node object's
        /// Value property will be set to</param>
        public static void Enqueue<T>(MyStack<T> inStack, MyStack<T> outStack, T newValue)
        {
            // Shift all outgoing stack nodes onto the incoming stack
            if (inStack.Length == 0)
            {
                while (outStack.Length > 0)
                {
                    inStack.Push(outStack.Pop());
                }
            }

            // Now that the nodes are in the correct order, push the new value onto the
            // incoming stack
            inStack.Push(newValue);
        }

        /// <summary>
        /// Dequeues from a "fake" queue making use of two MyStack objects holding
        /// Node objects of type <typeparamref name="T"/> and returns the removed
        /// Node's Value property
        /// </summary>
        /// <typeparam name="T">The type of values held in the provided stacks</typeparam>
        /// <param name="inStack">The stack representing incoming values</param>
        /// <param name="outStack">The stack representing outgoing values</param>
        /// <returns>The value of type <typeparamref name="T"/> that was held in
        /// the deqeueued Node object's Value property</returns>
        public static T Dequeue<T>(MyStack<T> inStack, MyStack<T> outStack)
        {
            // Shift all nodes from the incoming stack onto the outgoing stack
            if (outStack.Length == 0)
            {
                while (inStack.Length > 0)
                {
                    outStack.Push(inStack.Pop());
                }
            }

            // Now that the nodes are in the correct order, pop the top node of
            // the outgoing stack
            return outStack.Pop();
        }
    }
}