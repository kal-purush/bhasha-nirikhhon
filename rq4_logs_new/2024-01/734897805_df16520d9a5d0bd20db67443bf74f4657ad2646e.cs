class Program
{
    static void Main()
    {
        Console.Write("Enter a list of numbers, seperated by spaces: ");
        string? input = Console.ReadLine();

        int[] numbers = ParseInput(input);

        int[] evenNumbers = FilterEvenNumbers(numbers);
        
        Console.WriteLine("The even numbers are: " + string.Join(" ", evenNumbers));
        Console.ReadLine();
    }

    static int[] ParseInput(string? input)
    {
        string[] numberStrings = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        int[] numbers = new int[numberStrings.Length];
        for (int i = 0; i < numberStrings.Length; i++)
        {
            if (int.TryParse(numberStrings[i], out int number))
            {
                numbers[i] = number;
            }
            else
            {
                Console.WriteLine($"Invalid input: '{{numberStrings[i]}}' will be ignored. ");
            }
        }
        return numbers;
    }

    static int[] FilterEvenNumbers(int[] numbers)
    {
        return numbers.Where(n => n % 2 == 0).ToArray();
    }
}