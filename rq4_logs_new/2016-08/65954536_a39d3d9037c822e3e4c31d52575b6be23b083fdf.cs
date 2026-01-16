using System;

namespace TaxCalculator
{
    class Program
    {
        //these arrays is visible in all the static method, 
        //so you can use them in your method implementation

        static int[] minIncomeArray = new int[] 
            { 20000, 30000, 40000, 80000, 
              120000, 160000, 200000, 320000 };
        static double[] taxRateArray = new double[] 
            { 0.02, 0.035, 0.07, 0.115, 
              0.15, 0.17, 0.18, 0.20 };
        static int[] basePayableAmountArray = new int[] 
            { 0, 200, 550, 3350, 
              7950, 13950, 20750, 42350 };

        static void Main(string[] args)
        {
            int annualIncome = AskForIncome();
            int taxBracket = GetTaxBracket(annualIncome);
            double taxPayable = 
                CalculateIncomeTax(annualIncome, taxBracket);
            PrintResult(annualIncome, taxPayable);
        }

        //YOUR CODE HERE

        public static int AskForIncome()
        {
            Console.Write("Please enter your annual taxable income: ");
            int income = Convert.ToInt32(Console.ReadLine());
            return income;
        }

        public static int GetTaxBracket(int annualIncome)
        {
            int taxBracket = -1;
            for (int i = 0; i < minIncomeArray.Length; i++)
            {
                if (minIncomeArray[i] < annualIncome)
                {
                    taxBracket = i;
                }
            }

            return taxBracket;
        }

        public static double CalculateIncomeTax(int annualIncome, int taxBracket)
        {
            double payableTax = 0;
            if (taxBracket > -1) {
                payableTax = (annualIncome - minIncomeArray[taxBracket]) * taxRateArray[taxBracket]
                                    + basePayableAmountArray[taxBracket];
            }

            return payableTax;
        }

        public static void PrintResult(int annualIncome, double incomeTax)
        {
            Console.WriteLine("For taxable annual income of {0:c}, the tax payable amount is {1:c}",
                annualIncome, incomeTax);
        }
    }
}