using System;

namespace Exercise8
{
    class Program
    {
        
        static void Main(string[] args)
        {
           
            Console.WriteLine("Please enter annual interest rate of Your acoount: ");
            double annualRate = float.Parse(Console.ReadLine());
            Console.WriteLine("Please enter Your account balance at start: ");
            double balanceAtStart = float.Parse(Console.ReadLine());
            Console.WriteLine("Please enter number of months passed since: ");
            int monthsPassed = int.Parse(Console.ReadLine());
            SavingAccount myAccount = new SavingAccount(balanceAtStart, annualRate);
            myAccount.AddMonthlyInterestMoney(monthsPassed);
            for (int i = 1; i <= monthsPassed; i++)
            {
                Console.WriteLine($"Enter sum withdrawn during month {i}: ");
                myAccount.WithDraw(float.Parse(Console.ReadLine()));
                Console.WriteLine($"Enter sum deposited during month {i}: ");
                myAccount.AddDepositMoney(float.Parse(Console.ReadLine()));
            }

            Console.WriteLine($"Total deposited: {myAccount.TotalDeposited().ToString("0.00")} EUR");
            Console.WriteLine($"Total withdrawn: {myAccount.TotalWithdrawn().ToString("0.00")} EUR");
            Console.WriteLine($"Interest earned: {myAccount.TotalInterestEarned().ToString("0.00")} EUR");
            Console.WriteLine($"Ending balance: {myAccount.GetBalance().ToString("0.00")} EUR");           
            Console.ReadKey();
        }
    }
}