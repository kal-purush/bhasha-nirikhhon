using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataStructuresAssignment
{
    class Program
    {
        static void Main(string[] args)
        {
            int iAnswer = 0;
            do {
                Console.WriteLine("Please enter an integer 1-4.");
                Console.WriteLine("1. Stack\n2. Queue\n3. Dictionary\n4. Exit");
                
                Boolean bException = false;
                while(bException == false) {
                    try {
                        string sAnswer = Console.ReadLine();
                        iAnswer = Convert.ToInt32(sAnswer);
                        bException = true;
                    }
                    catch {
                        Console.WriteLine("Please enter an integer 1-4.");
                        Console.WriteLine("1. Stack\n2. Queue\n3. Dictionary\n4. Exit");
                    }
                }
            } while (iAnswer < 1 || iAnswer > 4);

            switch (iAnswer) {
                case 1:
                    Console.WriteLine("Stack");
                    break;
                case 2:
                    Console.WriteLine("Queue");
                    break;
                case 3:
                    Console.WriteLine("Dictionary");
                    break;
                case 4:
                    System.Environment.Exit(1);
                    break;
                default:
                    Console.WriteLine("Please enter an integer 1-4.");
                    Console.WriteLine("1. Stack\n2. Queue\n3. Dictionary\n4. Exit");
                    break;
            }

            string sEnd = Console.ReadLine();
        }
    }
}