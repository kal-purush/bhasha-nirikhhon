using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Homework1_2_5
{
    /// <summary>
    /// 2.5. Вывести название дня недели по его номеру (1 - понедельник, 7 - воскресенье) 
    /// </summary>
    class Program
    {
        /// <summary>
        /// массив дней недели
        /// </summary>
        private static string[] week = { "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье" };

        /// <summary>
        /// запрашиваем у пользователя день недели
        /// </summary>
        /// <returns>int день недели</returns>
        private static int GetDay()
        {
            string enter;
            while (true)
            {
                Console.Write("Введите число от 1 до 7: ");
                enter = Console.ReadLine();
                if (int.TryParse(enter, out int number))
                {
                    if (number >= 1 && number <= week.Length)
                    {
                        return number;
                    }
                }
            };
        }

        static void Main(string[] args)
        {
            int day = GetDay();
            Console.WriteLine(week[day - 1]);
        }
    }
}