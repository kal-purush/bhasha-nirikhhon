namespace OddEvenNumbers
{
    internal class Program
    {
        static void Main(string[] args)
        {
            int[] numbers = { 1, 2, 3, 4, 5, 6,7,8,9 };
            PrintNumbers("Numbers",numbers);
            PrintNumbers("Even",numbers.Where(a=>IsEven(a)));
            PrintNumbers("Odd",numbers.Where(a=>IsOdd(a)));
            Console.ReadKey();
            
        }
         static void PrintNumbers(string title,IEnumerable<int> numbers)
        {
            Console.WriteLine();
            Console.Write($"{title}:[");
            foreach (var n in numbers)
            {
                Console.Write($"{n}");
            }
            Console.Write($" ]");
            Console.WriteLine();
        }
        static bool IsEven(int number)
        {
            if (number % 2 == 0)
            {
                return true;
            }
            else
            {
                return false;   
            }
        }
        static bool IsOdd(int number)
        {
            if (number % 2 != 0)
            {
                return true;
            }
            else
            {
                return false;   
            }
        }

    }
}