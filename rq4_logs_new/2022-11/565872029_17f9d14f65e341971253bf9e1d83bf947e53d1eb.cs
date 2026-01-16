//Пользователь вводит с клавиатуры M чисел. Посчитайте, сколько чисел больше 0 ввёл пользователь.
using System;
using System.Collections.Generic;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            

            positiveNumbers();
        }

       static void positiveNumbers() //метод для подсчёта положительных чисел
        {
            List<int> arr = new List<int>();
            string input = " ";

            Console.Write("Type ");

            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("'q' ");
            Console.ResetColor();
            
            Console.WriteLine("to end number entry");

            for (int i = 0; ; i++ )
            {
                input = Console.ReadLine();

                if (input != "q")
                    arr.Add(Convert.ToInt32(input));
                else break;

            }

            int count = 0; //счётчик положительных чисел
            foreach(int i in arr)
            {
                if (i > 0) count++;
            }


            Console.WriteLine($"Положительных чисел введено: {count}");
            
        }
    }
}