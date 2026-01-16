using System.Linq.Expressions;

namespace _8_1
//Смоделируйте работу простого калькулятора.
//Программа должна запрашивать 2 целых числа, а затем – код операции (например, 1 – сложение, 2 – вычитание, 3 – произведение, 4 – частное).
//После этого на консоль выводится ответ. Используйте обработку деления на ноль (DivideByZeroException), нечислового ввода (FormatException).
{
    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Введите два целых числа");
                Console.Write("a=");
                int a = Convert.ToInt32(Console.ReadLine());
                Console.Write("b=");
                int b = Convert.ToInt32(Console.ReadLine());
                Console.WriteLine("Выберите операцию:");
                Console.WriteLine("1 - сложение");
                Console.WriteLine("2 - вычитание");
                Console.WriteLine("3 - произведение");
                Console.WriteLine("4 - частное");
                Console.Write("Введите код операции (1-4): ");
                int code = Convert.ToInt32(Console.ReadLine());
                double result = 0;
                switch (code)
                {
                    case 1:
                        result = a + b;
                        Console.WriteLine($"a + b = {result}");
                        break;
                    case 2:
                        result = a - b;
                        Console.WriteLine($"a - b = {result}");
                        break;
                    case 3:
                        result = a * b;
                        Console.WriteLine($"a * b = {result}");
                        break;
                    case 4:
                        result =  (double) a / b;
                        Console.WriteLine("a / b = {0:F3}", result);
                        break;
                    default:
                        Console.WriteLine("Ошибка: неверный код операции.Введите число от 1 до 4.");
                        break;
                }
            }
            catch (DivideByZeroException ex)
            {
                Console.WriteLine("Ошибка: деление на 0!");
            }
            catch (FormatException ex)
            {
                Console.WriteLine("Ошибка: введено не целое число!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Произошла неизвестная ошибка: {ex.Message}");
            }
            Console.ReadKey();
        }
    }
}