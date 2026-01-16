using System;
using StackAndQueue;

namespace TowersOfHanoi
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Please provide an integer greater than 0 of disks for the Tower of Hanoi:");

            int n = 0;

            // Make sure the user has provided a value of n > 0
            while (!Int32.TryParse(Console.ReadLine(), out n) || n < 1)
            {
                Console.WriteLine("\nPlease enter an integer greater than 0:");
            }

            MyQueue<string> moves = TowersOfHanoi(n);

            Console.WriteLine($"\nSolved the Tower of Hanoi in {moves.Length} moves:\n");

            while (moves.Length > 0)
            {
                Console.WriteLine(moves.Dequeue());
            }

            Console.WriteLine("\nPlease press any key to continue...");
            Console.ReadKey();
        }

        static MyQueue<string> TowersOfHanoi(int n)
        {
            MyQueue<string> moves = new MyQueue<string>();
            MyStack<int>[] stacks = new MyStack<int>[3] { new MyStack<int>(), new MyStack<int>(), new MyStack<int>() };

            for (int i = n; i > 0; i--)
            {
                stacks[0].Push(i);
            }

            // Starting move is based on the evenness of the number
            if (n % 2 != 0)
            {
                stacks[2].Push(stacks[0].Pop());
                moves.Enqueue("Disk 1 moved from A to C.");
            }
            else
            {
                stacks[1].Push(stacks[0].Pop());
                moves.Enqueue("Disk 1 moved from A to B.");
            }

            // Keeps disks from moving from even to even or odd to odd
            bool[] stacksEven = new bool[3];
            // Prevents the loop 
            int lastDisk = 1;

            int[] stackValues = new int[3];


            // Keep moving until stack C (index 2) gains all of stack A's disks
            while (stacks[2].Length < n)
            {
                int source = -1;
                int destination = -1;

                stackValues[0] = stacks[0].Length > 0 ? stacks[0].Peek() : 0;
                stackValues[1] = stacks[1].Length > 0 ? stacks[1].Peek() : 0;
                stackValues[2] = stacks[2].Length > 0 ? stacks[2].Peek() : 0;
                stacksEven[0] = stackValues[0] % 2 == 0;
                stacksEven[1] = stackValues[1] % 2 == 0;
                stacksEven[2] = stackValues[2] % 2 == 0;

                for (int i = 0; i < 3; i++)
                {
                    // If this wasn't the last disk moved and the source stack isn't empty
                    if (stackValues[i] != lastDisk && stackValues[i] > 0)
                    {
                        // Check for destinations
                        for (int j = 0; j < 3; j++)
                        {
                            if (i != j && (stackValues[i] < stackValues[j] || stackValues[j] < 1))
                            {
                                // Prefer non-empty destination if two empty stacks are available
                                if (destination >= 0 && stackValues[j] < 1)
                                {
                                    break;
                                }

                                if (stacksEven[i] != stacksEven[j] || stackValues[j] < 1)
                                {
                                    source = i;
                                    destination = j;
                                }
                            }
                        }
                    }
                }

                if (source >= 0 && destination >= 0)
                {
                    // Adding 'A' (0x41) and i or j will map to 'A', 'B', or 'C'
                    char startStack = Convert.ToChar('A' + source);
                    char endStack = Convert.ToChar('A' + destination);
                    int movingDisk = stacks[source].Pop();

                    stacks[destination].Push(movingDisk);
                    lastDisk = movingDisk;
                    moves.Enqueue($"Disk {movingDisk} moved from {startStack} to {endStack}.");
                }
            }

            return moves;
        }
    }
}