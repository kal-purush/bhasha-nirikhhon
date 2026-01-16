static class Program
{
    static void Main()
    {
        double rateOfReturn;
        int years;
        
        Console.Write("What is the rate of return? ");

        do
        {
            if (!double.TryParse(Console.ReadLine(), out rateOfReturn))
            {
                Console.WriteLine("Sorry, that is not a valid input. Please enter a numeric value. ");
                continue;
            }
            
            if (rateOfReturn == 0)
            {
                Console.WriteLine("Sorry, that is not a valid input. Rate of return can not be zero. ");
            }
        } while (rateOfReturn == 0);

        years = (int)(72 / rateOfReturn);
        
        Console.WriteLine($"It will takes {years} years to double your investment. ");
    }
}