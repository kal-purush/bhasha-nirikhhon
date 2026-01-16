using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace it_sadok_hw
{
    class Library
    {
        private int StartedDeposit = 10000; // the amount of books when Library opened 

        public void Add(int a) // call method if reader's reply has "take"
        {
            int result = StartedDeposit - a;
            Console.WriteLine($"Thank You. You take {a} book(s), and our depositary for now is {result} items!");
            Console.WriteLine("Have a good time! See you back soon!");
            Console.ReadLine();
        }

        public void Remove(int b) // call method if reader's reply has "back"
        {
            int result = StartedDeposit + b;
            Console.WriteLine($"Thank You. You give us back {b} book(s), and our depositary for now is {result} items!");
            Console.WriteLine("See you back soon!");
            Console.ReadLine();
        }
    }
}