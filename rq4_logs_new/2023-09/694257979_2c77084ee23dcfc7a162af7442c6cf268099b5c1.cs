using System;

class Program
{
    static void Main(string args)
    {
        int operation;
        do
        {
            Console.WriteLine("Выберите операцию:\n1. Сложение\n2. Вычитание\n3. Умножение\n4. Деление\n5. Возведение в степень\n6. Квадратный корень\n7. 1 процент от числа\n8. Факториал\n9. Выйти из программы");
            operation = Convert.ToInt32(Console.ReadLine());

            if (operation >= 1 && operation <= 8)
            {
                Console.WriteLine("Введите циферку:)");
                double num1 = Convert.ToDouble(Console.ReadLine());
                double num2;
                switch (operation)
                {
                    case 1:
                        Console.WriteLine("Введите вторую циферку:)");
                        num2 = Convert.ToDouble(Console.ReadLine());
                        Console.WriteLine("Ответииик: " + (num1 + num2));
                        break;
                    case 2:
                        Console.WriteLine("Введите вторую чиселку :))");
                        num2 = Convert.ToDouble(Console.ReadLine());
                        Console.WriteLine("Результат: " + (num1 - num2));
                        break;
                    case 3:
                        Console.WriteLine("Введите вторую цифрууу:::)");
                        num2 = Convert.ToDouble(Console.ReadLine());
                        Console.WriteLine("Результат: " + (num1 * num2));
                        break;
                    case 4:
                        Console.WriteLine("Введите вторую циферку)");
                        num2 = Convert.ToDouble(Console.ReadLine());
                        Console.WriteLine("Результат: " + (num1 / num2));
                        break;
                    case 5:
                        Console.WriteLine("Введите степенюшку");
                        num2 = Convert.ToDouble(Console.ReadLine());
                        Console.WriteLine("Результат: " + Math.Pow(num1, num2));
                        break;
                    case 6:
                        Console.WriteLine("Результат: " + Math.Sqrt(num1));
                        break;
                    case 7:
                        Console.WriteLine("Результат: " + (num1 * 0.01));
                        break;
                    case 8:
                        int fact = 1;
                        for (int i = 1; i <= num1; i++)
                        {
                            fact *= i;
                        }
                        Console.WriteLine("Результат: " + fact);
                        break;
                }
            }
        } while (operation != 9);
    }
}
