using Microsoft.IdentityModel.Tokens;
using rb.bll;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rb.console
{
    internal class LoginMenu
    {
        public static void Print()
        {
            Console.Clear();
            Console.WriteLine("Login\n");

            Console.Write("Username: ");
            string Username = Console.ReadLine();
            Console.Write("Password: ");
            string Password = Console.ReadLine();

            if (Username.IsNullOrEmpty() || Password.IsNullOrEmpty())
            {
                Console.WriteLine("\nAll input required!");
                Console.ReadKey();
                Print();
            }

            if (!UserService.VerifyUser(Username, Password))
            {
                Console.WriteLine("\nWrong user info!");
                Console.ReadKey();
                Print();
            }

            Console.WriteLine("\nUser Logged In");
            Console.ReadKey();
            MainMenu.Print();
        }
    }
}