using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lesson2
{
    class Program
    {
        static void Main(string[] args)
        {
            string userLogin = "user123";
            string userPassword = "QwErTy";
            int attemptsCount = 0;
            bool isAuthorized = false;

            Console.Clear();

            while (attemptsCount < 3 && !isAuthorized)
            {
                attemptsCount += 1;

                Console.WriteLine("\r\nEnter your login:");
                string login = Console.ReadLine();

                Console.WriteLine("\r\nEnter your password:");
                string password = Console.ReadLine();

                if (userLogin.ToUpper() == login.ToUpper() && userPassword == password)
                {
                    isAuthorized = true;
                    break;
                }

                Console.WriteLine("\r\nIncorrect login or password. Try again.");
            }

            if (isAuthorized) {
                Console.WriteLine("\r\nSuccessful authorization. Welcome.");
            } else {
                Console.WriteLine("\r\nAttempts are over. Please, try again later.");
            }
            Console.ReadLine();

        }
    }
}