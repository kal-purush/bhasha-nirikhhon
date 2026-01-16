namespace stack_and_queue
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Stack<int> stack = new Stack<int>();

            // Pushing values onto the stack
            stack.Push(10);
            stack.Push(20);
            stack.Push(30);

            // Checking if the stack is empty
            bool isStackEmpty = stack.IsEmpty();
            Console.WriteLine("Is Stack Empty? " + isStackEmpty);

            // Peeking the top value of the stack
            int topValue = stack.Peek();
            Console.WriteLine("Top value of Stack: " + topValue);

            // Popping values from the stack
            int poppedValue1 = stack.Pop();
            int poppedValue2 = stack.Pop();
            Console.WriteLine("Popped values: " + poppedValue1 + ", " + poppedValue2);

            // Creating a Queue
            Queue<string> queue = new Queue<string>();

            // Enqueueing values into the queue
            queue.Enqueue("ahmad");
            queue.Enqueue("jamal");
            queue.Enqueue("khater");

            // Checking if the queue is empty
            bool isQueueEmpty = queue.IsEmpty();
            Console.WriteLine("Is Queue Empty? " + isQueueEmpty);

            // Peeking the front value of the queue
            string frontValue = queue.Peek();
            Console.WriteLine("Front value of Queue: " + frontValue);

            // Dequeueing values from the queue
            string dequeuedValue1 = queue.Dequeue();
            string dequeuedValue2 = queue.Dequeue();
            Console.WriteLine("Dequeued values: " + dequeuedValue1 + ", " + dequeuedValue2);

        }
    }
}