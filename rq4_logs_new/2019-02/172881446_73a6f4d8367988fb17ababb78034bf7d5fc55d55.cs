using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*      11. С клавиатуры вводятся числа, пока не будет введён 0. Подсчитать среднее арифметическое всех положительных чётных чисел, оканчивающихся на 8.

    Александр Кушмилов
*/

namespace Task_11
{
    class Program
    {
        /// <summary>
        /// Метод запрашивает строку пока введеные данные не будут сконвертированны в число
        /// </summary>
        /// <returns></returns>
        static int CheckNumbers()
        {
            bool check = false;
            int y = 0;
            do
            {
                Console.WriteLine("Введите число или \"0\" для окончания ввода:");
                check = Int32.TryParse(Console.ReadLine(), out y);
            } while (!check);
            return y;
        }

        /// <summary>
        /// Метод находит среднее арифметическое коллекции
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        static int Average(List<int> list)
        {
            return list.Sum() / list.Count;
        }

        /// <summary>
        /// Метод возвращает коллекцию, всех положительных чётных чисел, оканчивающихся на 8, взятых из исходной коллекции.
        /// </summary>
        /// <param name="List">Исходная коллекция</param>
        /// <returns></returns>
        private static List<int> RemoveX(List<int> List)
        {
            List<int> buf = new List<int>();
            foreach (var item in List)
            {
                if (item > 0 && item % 10 == 8) buf.Add(item);
            }
            return buf;
        }


        static void Main(string[] args)
        {
            List<int> numList = new List<int>();
            do
            {
                int buf = CheckNumbers();
                if (buf != 0)
                {
                    numList.Add(buf);
                }
                else
                {
                    break;
                }
            } while (true);
            Console.WriteLine();
            foreach (var item in numList)
            {
                Console.WriteLine(item);
            }
            Console.WriteLine();

            numList = RemoveX(numList);
            foreach (var item in numList)
            {
                Console.WriteLine(item);
            }
            Console.WriteLine();

            int average = Average(numList);
            Console.WriteLine(average);

            Console.ReadKey();
        }

    }
}