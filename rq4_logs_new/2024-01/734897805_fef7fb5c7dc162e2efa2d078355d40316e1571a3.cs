class Program
{
    static void Main()
    {
        List<int> responseTimes = new List<int>();

        while (true)
        {
            Console.Write("Enter a number (or 'done' to finish the sequence): ");
            string input = Console.ReadLine();

            if (input.ToLower() == "done")
                break;
            if (int.TryParse(input, out int responseTime))
            {
                responseTimes.Add((responseTime));
            }
            else
            {
                Console.WriteLine("Invlaid input. Please enter a valid number or 'done'.");
            }
        }

        if (responseTimes.Count > 0)
        {
            Console.WriteLine($"Numbers: {string.Join(", ", responseTimes)}. ");
            Console.WriteLine($"The average is {CalculateAverage(responseTimes)}. ");
            Console.WriteLine($"The minimum is {responseTimes.Min()}. ");
            Console.WriteLine($"The maximum is {responseTimes.Max()}. ");
            Console.WriteLine($"The standard deviation is {CalculateStandardDeviation(responseTimes)}. ");
        }
        else
        {
            Console.WriteLine("No response times entered... exiting program");
        }
        Console.ReadLine();
    }

    static double CalculateAverage(List<int> numbers)
    {
        return numbers.Count > 0 ? numbers.Average() : 0;
    }

    static double CalculateStandardDeviation(List<int> numbers)
    {
        if (numbers.Count <= 1)
            return 0;

        double mean = CalculateAverage(numbers);
        double sumOfSquares = 0;
        
        foreach (int number in numbers)
        {
            double difference = number - mean;
            sumOfSquares += difference * difference;
        }

        double variance = sumOfSquares / (numbers.Count - 1);
        return Math.Sqrt(variance);
    }
}