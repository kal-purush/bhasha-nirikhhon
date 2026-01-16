namespace Task4_8
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Введите число n.");
            long n = Convert.ToInt64(Console.ReadLine());
            long number = 0;
            long n2 = n;
            if (n < 0)
            {
                n2 = -n;
            }
            do
            {
                n2 /= 10;
                number++;
            }
            while (n2 % 10 > 0) ;
            long n3 = n;
            if (n < 0)
            {
                n3 = -n;
                Console.Write("-");
            }
            for (long i = 1; i <= number; i++)
            {
                Console.Write(n3%10);
                n3 /= 10;
            }
            Console.ReadKey();
        }
    }
}