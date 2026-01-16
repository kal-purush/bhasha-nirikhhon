namespace Multiples3sAnd5s
{
    internal class Challenge1Multiple3sAnd5s
    {
        static void Main(string[] args)
        {
            List<int> numbers = new List<int>();
            int numberToBeAdded = 0;
            Console.WriteLine(numberToBeAdded % 3 == 0);
            for (int i = 0; i <= 1000; i++)
            {
                numberToBeAdded++;
                if (numberToBeAdded % 3 == 0 || numberToBeAdded % 5 == 0)
                {
                   numbers.Add(numberToBeAdded);
                    Console.WriteLine(numberToBeAdded);

                }
                
            }
            Console.WriteLine(numbers.Sum());
        }
    }
}