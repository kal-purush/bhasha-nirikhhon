namespace Task4_4
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Введите целое число а>0:");
            int a = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("Введите целое число b<0:");
            int b = Convert.ToInt32(Console.ReadLine());
            if (a <= 0 || b >= 0)
            {
                Console.WriteLine("Числа не соответсвует условию.");
            }
            else
            {
                int absB = -b;
                double power = 1;
                for (int i = 1; i <= absB; i++)
                {
                    power *= a;
                }
                power = 1 / power;
                Console.WriteLine($"{a}^({b})={power}");
                Console.ReadKey();
            }
        }
    }
}