using System;
using System.Collections.Generic;
using System.Text;

namespace ProjectBank.Views
{
    class CreateAccountView
    {
        public static void Run()
        {
            Console.Clear();

            Console.WriteLine("Create account");

            Console.Write("Account Name:");
            string accountName = Console.ReadLine();
            Console.Write("Account number:");

            int accountNumber;
            while (!int.TryParse(Console.ReadLine(), out accountNumber))
            {
                Console.WriteLine("You Can only use numbers");
                Console.Write("Account number:");
            }
            
            Session.User.AddAcc(new Accounts(accountName, accountNumber,0));

            DashboardView.Run();
        }


    }
}