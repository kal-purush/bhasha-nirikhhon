namespace BUKEP.Student.ConsoleCalculator
{
    internal class Program
    {
        static void Main(string[] args)
        {
            ConsoleCalculator();
        }
        static void ConsoleCalculator()
        {
            while (true)
            {
                Console.WriteLine("Введите простое математическое выражение для вычисления операции (+ или - или * или /) над двумя числами.\nПо завершению ввода операции нажмите Enter:");
                string input = Console.ReadLine();

                string[] words = input.Split(new char[] { '+', '-', '/', '*' });

                try
                {
                    int numberOne = int.Parse(words[0]);
                    int numberTwo = int.Parse(words[1]);

                    Calc(input, numberOne, numberTwo);
                }
                catch
                {
                    Console.WriteLine("Введено некорректное выражение!");
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


        static void Calc(string input, int numberOne, int numberTwo)
        {
            foreach (char mathSymbol in input)
            {
                switch (mathSymbol)
                {
                    case '+':
                        Console.WriteLine($"{numberOne} + {numberTwo} = {numberOne + numberTwo}");
                        break;
                    case '-':
                        Console.WriteLine($"{numberOne} - {numberTwo} = {numberOne - numberTwo}");
                        break;
                    case '*':
                        Console.WriteLine($"{numberOne} * {numberTwo} = {numberOne * numberTwo}");
                        break;
                    case '/':
                        if (numberOne == 0 || numberTwo == 0)
                        {
                            Console.WriteLine("На ноль делить нельзя!");
                        }
                        else
                        {
                            Console.WriteLine($"{numberOne} / {numberTwo} = {(float)(numberOne / (float)numberTwo)}");
                        }
                        break;
                }

            }
        }
    }
}