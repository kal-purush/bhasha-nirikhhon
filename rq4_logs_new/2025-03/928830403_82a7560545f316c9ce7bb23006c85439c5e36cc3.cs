namespace Task5_01
//Сформировать одномерный массив из 10 случайных чисел из диапазона [0, 100]. Определить, каких чисел больше – четных или нечетных
{
    internal class Program
    {
        static void Main(string[] args)
        {
            const int n = 10;
            int[] array = new int[n];
            Random rnd = new Random();
            int count = 0;
            Console.Write($"{n} случайных числел: ");
            for (int i = 0; i < n; i++)
            {
                array[i] = rnd.Next(0, 101);
                Console.Write($"{array[i]} ");
                if (array[i] % 2 == 0)
                {
                    count++;
                }
                else
                {
                    count--;
                }
            }
            Console.WriteLine();
            if (count == 0)
            {
                Console.WriteLine("Количество четных и нечетных чисел - одинаково.");
            }
            else if (count > 1)
            {
                Console.WriteLine("Четных числел больше, чем нечетных.");
            }
            else
            {
                Console.WriteLine("Четных числел меньше, чем нечетных.");
            }
            Console.ReadKey();
        }
    }
}