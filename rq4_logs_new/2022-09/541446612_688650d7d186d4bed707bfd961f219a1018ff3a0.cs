namespace exercise6
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int num = 1; num <= 100; num++)
            {


                if (num % 3 == 0 && num % 5 == 0)
                {
                    Console.ForegroundColor = ConsoleColor.DarkBlue;
                    Console.WriteLine($"{num}:Electric and Fire");
                }
                else if (num % 3 == 0 && num % 5 != 0)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"{num}:Fire");
                }
                else if (num % 5 == 0 && num % 3 != 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"{num}:Electric");
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Black;
                    Console.WriteLine($"{num}: Normal");
                }
            }
        }
    } 
}