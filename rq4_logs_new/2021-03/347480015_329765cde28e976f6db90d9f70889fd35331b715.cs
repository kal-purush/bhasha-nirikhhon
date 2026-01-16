// Андрей Никеенко
// 2. а) С клавиатуры вводятся числа, пока не будет введен 0 (каждое число в новой строке). Требуется подсчитать сумму всех нечетных положительных чисел. Сами числа и сумму вывести на экран, используя tryParse;
//    б) Добавить обработку исключительных ситуаций на то, что могут быть введены некорректные данные. При возникновении ошибки вывести сообщение. Напишите соответствующую функцию;

using System;

namespace dz3_2
{
    class Program
    {
        public static double GetNum()
        {
            string str;
            double num;
            bool flag;

            do
            {
                str = Console.ReadLine();
                flag = double.TryParse(str, out num);
                if (flag == false) Console.WriteLine("Это не число! Давайте посерьёзней.");
            }
            while (!flag);

            return num;
        }
        static void Main(string[] args)
        {
            double sum = 0;
            string str = "";

            Console.WriteLine("Вводите любые числа. Для окончания ввода введите 0.");

            while (true)
            {
                double num = GetNum();
                if (num == 0) break;
                else if (num > 0 && num % 1 == 0 && num % 2 != 0)
                {
                    sum += num;
                    str += " " + Convert.ToString(num);
                }
            }

            Console.WriteLine($"\nНечетные положительные числа: {str}");
            Console.WriteLine($"Сумма чисел равна: {sum}");

            Console.ReadKey();
        }
    }

}