using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AuthorizeLocker.DBLayer;

namespace Client
{
    class Program
    {
        static void Main(string[] args)
        {
            bool again = true;

            while (again)
            {
                Console.WriteLine("Enter your credentials:");
                Console.Write("Login > ");
                string login = Console.ReadLine();
                Console.WriteLine();

                Console.Write("Password > ");
                string password = Console.ReadLine();
                Console.WriteLine();

                Console.WriteLine("Cheking your credentials...");
                if (DbManager.Login(login, password))
                {
                    var oldVal = Console.ForegroundColor;

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("### SUCCESS ###");

                    Console.ForegroundColor = oldVal;
                }
                else
                {
                    var oldVal = Console.ForegroundColor;

                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("### FAIL ###");

                    Console.ForegroundColor = oldVal;
                }
                
                
                var key = Console.ReadKey();
                if (key.Key == ConsoleKey.Escape)
                    again = false;

                Console.WriteLine();
            }
            
        }
    }
}