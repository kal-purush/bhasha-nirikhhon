using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*  10. Дано целое число N > 0. С помощью операций деления нацело и взятия остатка от деления определить,
        имеются ли в записи числа N нечётные цифры.Если имеются, то вывести True, если нет – вывести False.

    Александр Кушмилов
*/

namespace Task_10
{
    class Program
    {
        static void Main(string[] args)
        {
            bool res = false;
            Console.WriteLine("Введите целое число N > 0");
            res = GoodNumber(Convert.ToInt32(Console.ReadLine()));
            Console.WriteLine($"В записи числа N имеются нечётные цифры - {res}");
            Console.ReadKey();
        }

        /// <summary>
        /// Метод определяет имеются ли в записи числа нечётные цифры.
        /// </summary>
        /// <param name="v">Проверяемое число</param>
        /// <returns></returns>
        private static bool GoodNumber(int v)
        {
            while (v != 0)
            {
                if (v % 2 != 0) return true;
                v /= 10;
                Console.WriteLine(v);
            }
            return false;
        }
    }
}