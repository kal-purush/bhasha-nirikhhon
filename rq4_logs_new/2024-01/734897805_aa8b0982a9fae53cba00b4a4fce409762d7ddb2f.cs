namespace Exercise26
{
    static class Program
    {
        static void Main(string[] args)
        {
            Console.Write("What is your balance? ");
            double balance = Convert.ToDouble(Console.ReadLine());

            Console.Write("What is the APR of the card (as a percent)? ");
            double apr = Convert.ToDouble(Console.ReadLine());

            Console.Write("What is the monthly payment you can make? ");
            double monthlyPayment = Convert.ToDouble(Console.ReadLine());
            
            double dailyRate = apr / 100 / 365;
            
            double months = (-1.0 / 30.0) * Math.Log(1 + balance / monthlyPayment * (1 - Math.Pow(1 + dailyRate, 30))) / Math.Log(1 + dailyRate);

            Console.WriteLine($"It will take you {Math.Ceiling(months)} months to pay off this card.");
        }
    }
}