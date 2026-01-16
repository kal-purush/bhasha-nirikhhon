using System;

namespace Project2
{
    class Program
    {
        static void Main(string[] args)
        {
            string[,] direct = new string[5, 2];
            for (int i = 0; i < 5; ++i)
            {
                Console.WriteLine($"Запись №{i + 1}");
                Console.Write("Введите имя контакта: ");
                direct[i, 0] = Console.ReadLine();
                Console.Write("Введите e-mail: ");
                direct[i, 1] = Console.ReadLine();
                Console.Clear();

            }
            for (int i = 0; i < 5; ++i)
            {

                for (int j = 0; j < 2; ++j)
                {
                    Console.Write(direct[i, j]+" ");

                }
                Console.WriteLine();
            }
            
        }
    }
}