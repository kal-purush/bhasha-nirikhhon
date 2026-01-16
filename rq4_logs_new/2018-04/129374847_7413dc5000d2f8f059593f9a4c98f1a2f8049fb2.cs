using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            string userInput;
            /// <summary>
            /// Запрашиваем у пользователя строку на ввод
            /// </summary>
            Console.WriteLine("Введите мяу или гав:"); Console.Write("\n->");
            userInput = Console.ReadLine();
            userInput = userInput.ToLower();
            userInput = userInput.Trim();
            /// <summary>
            /// Обрабатываем строку и выводим соответствующий ответ
            /// </summary>
            if (userInput == "мяу")
                Console.WriteLine("Покорми кота");
            if(userInput == "гав")
                Console.WriteLine("Погуляй с собакой");
            else
                Console.WriteLine("Вы ввели: {0}", userInput);
            Console.ReadKey();
        }
    }
}