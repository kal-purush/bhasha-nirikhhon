namespace BUKEP.Student.ConsoleCalculator
{
    internal class Program
    {
        static void Main(string[] args)
        {

            while (true)
            {
                Console.WriteLine("\r\nВведите простое математическое выражение для вычисления операции (+ или - или * или /) над двумя числами.\nПо завершению ввода операции нажмите Enter:");
                string mathExpression = Console.ReadLine();

                char[] symbolsMath = new char[] { '+', '-', '/', '*' };
                string[] numbers = mathExpression.Split(symbolsMath);
                if (numbers.Length >= 3)
                {
                    Console.WriteLine("В выражении должно быть только 2 числа");
                    continue;
                }
                try
                {
                    int numberOne = int.Parse(numbers[0]);
                    int numberTwo = int.Parse(numbers[1]);
                    char s = symbolsMath.First(s => mathExpression.Any(os => os == s));
                    {
                        switch (s)
                        {
                            case '+':
                                Console.WriteLine($"{numberOne} + {numberTwo} = {numberOne + numberTwo}");
                                break;
                            case '-':
                                Console.WriteLine($"{numberOne} - {numberTwo} = {numberOne - numberTwo}");
                                break;
                            case '/':
                                if (numberTwo == 0)
                                    Console.WriteLine("На 0 делить нельзя");
                                else
                                    Console.WriteLine($"{numberOne} / {numberTwo} = {(float)(numberOne / (float)numberTwo)}");
                                break;
                            case '*':
                                Console.WriteLine($"{numberOne} * {numberTwo} = {numberOne * numberTwo}");
                                break;

                        }
                    }

                }
                catch
                {
                    Console.WriteLine("Неверный ввод!");
                }
                while (true)
                {
                    Console.WriteLine();
                    Console.WriteLine("Для повторного ввода операции нажмите Enter, для завершения приложения Esc.");
                    ConsoleKeyInfo keyInfo = Console.ReadKey();

                    if (keyInfo.Key == ConsoleKey.Escape)
                    {
                        return;
                    }
                    else if (keyInfo.Key == ConsoleKey.Enter)
                    {
                        break;
                    }
                }
            
            }

        }
    }
}