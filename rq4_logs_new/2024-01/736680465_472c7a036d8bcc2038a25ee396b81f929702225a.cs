namespace BUKEP.Student.SharpInstructions
{
    internal class Program
    {
        static void Main(string[] args)
        {
            static void MyMethod()
            {
                while (true)
                {
                    Console.WriteLine();
                    string inscription = "Для вызова выполняемой подпрограммы укажите ее номер и нажните Enter:";
                    Console.WriteLine(inscription);
                    Console.WriteLine("1 - IF ELSE\r\n2 - WHILE\r\n3 - DO WHILE\r\n4 - FOR\r\n5 - FOREACH\r\n6 - SWITCH\n");

                    int userImput;
                    try
                    {
                        userImput = Convert.ToInt32(Console.ReadLine());
                    }
                    catch
                    {
                        Console.WriteLine("Некоректный ввод!");
                        continue;
                    }



                    Console.Clear();
                    switch (userImput)
                    {

                        case 1:
                            while (true)
                            {
                                Console.WriteLine();
                                Console.WriteLine("1 - IF ELSE");
                                Console.WriteLine("Для начала работы нажмите Enter. Для выхода в меня нажмите Escape.");
                                ConsoleKeyInfo ImputCommand = Console.ReadKey();
                                Console.WriteLine();
                                if (ImputCommand.Key == ConsoleKey.Enter)
                                {
                                    try
                                    {
                                        Console.WriteLine();
                                        Console.WriteLine("Для выполнения подпрограммы IF ELSE введите два числа которые будут сравниваться.\nПосле ввода значения нажмите Enter.");
                                        Console.Write("Введите первое число: ");
                                        int numberOne = Convert.ToInt32(Console.ReadLine());
                                        Console.Write("Введите первое число: ");
                                        int numberTwo = Convert.ToInt32(Console.ReadLine());

                                        IFELSESubroutine(numberOne, numberTwo);
                                        continue;
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Ошибка ввода!");
                                        continue;
                                    }
                                }
                                else if (ImputCommand.Key == ConsoleKey.Escape)
                                {
                                    Console.Write("ВВы вернулись в меню.");
                                    break;
                                }
                                else
                                {
                                    Console.WriteLine("Введена неверная команда!");
                                    continue;
                                }
                            }
                            break;
                        case 2:
                            while (true)
                            {
                                Console.WriteLine();
                                Console.WriteLine("2 - WHILE");
                                Console.WriteLine("Для начала работы нажмите Enter. Для выхода в меня нажмите Escape.");
                                ConsoleKeyInfo ImputCommand = Console.ReadKey();
                                Console.WriteLine();
                                if (ImputCommand.Key == ConsoleKey.Enter)
                                {
                                    try
                                    {
                                        Console.WriteLine();
                                        Console.WriteLine("Для выполнения подпрограммы WHILE введите слово или символ и количество повторений.\nПосле ввода значения нажмите Enter.");
                                        Console.Write("Введите слово или символ: ");
                                        string line = Console.ReadLine();
                                        Console.Write("Введите количество повторений: ");
                                        int numberRepetitions = Convert.ToInt32(Console.ReadLine());

                                        WHILESubroutine(line, numberRepetitions);
                                        continue;
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Ошибка ввода!");
                                        continue;
                                    }
                                }
                                else if (ImputCommand.Key == ConsoleKey.Escape)
                                {
                                    Console.Write("ВВы вернулись в меню.");
                                    break;
                                }
                                else
                                {
                                    Console.WriteLine("Введена неверная команда!");
                                    continue;
                                }
                            }
                            break;
                        case 3:
                            while (true)
                            {
                                Console.WriteLine();
                                Console.WriteLine("3 - DO WHILE");
                                Console.WriteLine("Для начала работы нажмите Enter. Для выхода в меня нажмите Escape.");
                                ConsoleKeyInfo ImputCommand = Console.ReadKey();
                                Console.WriteLine();
                                if (ImputCommand.Key == ConsoleKey.Enter)
                                {
                                    try
                                    {
                                        Console.WriteLine();
                                        Console.WriteLine("Для выполнения подпрограммы DO WHILE введите число (условие выхода из цикла 0).\nПосле ввода значения нажмите Enter.");

                                        DOWHILESubroutine();
                                        continue;
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Ошибка ввода!");
                                        continue;
                                    }
                                }
                                else if (ImputCommand.Key == ConsoleKey.Escape)
                                {
                                    Console.Write("ВВы вернулись в меню.");
                                    break;
                                }
                                else
                                {
                                    Console.WriteLine("Введена неверная команда!");
                                    continue;
                                }
                            }
                            break;
                        case 4:
                            while (true)
                            {
                                Console.WriteLine();
                                Console.WriteLine("4 - FOR");
                                Console.WriteLine("Для начала работы нажмите Enter. Для выхода в меня нажмите Escape.");
                                ConsoleKeyInfo ImputCommand = Console.ReadKey();
                                Console.WriteLine();
                                if (ImputCommand.Key == ConsoleKey.Enter)
                                {
                                    try
                                    {
                                        Console.WriteLine();
                                        Console.WriteLine("Для выполнения подпрограммы FOR введите начальное число и количество повторений.\nПосле ввода значения нажмите Enter.");
                                        Console.Write("Введите начальное число: ");
                                        int seedNumber = Convert.ToInt32(Console.ReadLine());
                                        Console.Write("Ведеите число повторений: ");
                                        int numberRepetitions = Convert.ToInt32(Console.ReadLine());

                                        if (seedNumber >= numberRepetitions)
                                        {
                                            Console.WriteLine("Первое число больше или равно второму числу. Цикл не работает!");
                                            continue;
                                        }
                                        else
                                        {
                                            FORSubroutine(seedNumber, numberRepetitions);
                                        }

                                        continue;
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Ошибка ввода!");
                                        continue;
                                    }
                                }
                                else if (ImputCommand.Key == ConsoleKey.Escape)
                                {
                                    Console.Write("ВВы вернулись в меню.");
                                    break;
                                }
                                else
                                {
                                    Console.WriteLine("Введена неверная команда!");
                                    continue;
                                }
                            }
                            break;
                        case 5:
                            while (true)
                            {
                                Console.WriteLine();
                                Console.WriteLine("5 - FOREACH");
                                Console.WriteLine("Для начала работы нажмите Enter. Для выхода в меня нажмите Escape.");
                                ConsoleKeyInfo ImputCommand = Console.ReadKey();
                                Console.WriteLine();
                                if (ImputCommand.Key == ConsoleKey.Enter)
                                {
                                    try
                                    {
                                        Console.WriteLine();
                                        Console.WriteLine("Для выполнения подпрограммы FOREACH введите строку и она будет разобрана на символы.\nПосле ввода значения нажмите Enter.");
                                        Console.Write("Введите строку: ");
                                        string line = Console.ReadLine();

                                        FOREACHSubroutine(line);
                                        continue;
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Ошибка ввода!");
                                        continue;
                                    }
                                }
                                else if (ImputCommand.Key == ConsoleKey.Escape)
                                {
                                    Console.Write("ВВы вернулись в меню.");
                                    break;
                                }
                                else
                                {
                                    Console.WriteLine("Введена неверная команда!");
                                    continue;
                                }
                            }
                            break;
                        case 6:
                            while (true)
                            {
                                Console.WriteLine();
                                Console.WriteLine("6 - SWITCH");
                                Console.WriteLine("Для начала работы нажмите Enter. Для выхода в меня нажмите Escape.");
                                ConsoleKeyInfo ImputCommand = Console.ReadKey();
                                Console.WriteLine();
                                if (ImputCommand.Key == ConsoleKey.Enter)
                                {
                                    try
                                    {
                                        Console.WriteLine();
                                        Console.WriteLine("Для выполнения подпрограммы SWITCH введите порядковый номер месяца.\nПосле ввода значения нажмите Enter.");
                                        Console.Write("Введите первое число: ");
                                        int mounthNumber = Convert.ToInt32(Console.ReadLine());

                                        SWITCHSubroutine(mounthNumber);
                                        continue;
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Ошибка ввода!");
                                        continue;
                                    }
                                }
                                else if (ImputCommand.Key == ConsoleKey.Escape)
                                {
                                    Console.Write("ВВы вернулись в меню.");
                                    break;
                                }
                                else
                                {
                                    Console.WriteLine("Введена неверная команда!");
                                    continue;
                                }
                            }
                            break;
                        default:
                            Console.WriteLine();
                            Console.WriteLine("Такой программы нет!");
                            break;
                    }


                }
            }

            static void FORSubroutine(int seedNumber, int numberOfRepetitions)
            {
                //Реализация цикла FOR
                Console.WriteLine("Результат:");
                for (int i = seedNumber; i < numberOfRepetitions + 1; i++)
                {
                    Console.WriteLine(i);
                }
                Console.WriteLine("Подпрограмма FOR завершена.");
            }

            static void IFELSESubroutine(int numberOne, int numberTwo)
            {
                Console.WriteLine("Решение");
                if (numberOne < numberTwo)
                {
                    Console.WriteLine($"Число: {numberOne} меньше числа: {numberTwo}");
                }
                else if (numberOne > numberTwo)
                {
                    Console.WriteLine($"Число: {numberOne} больше числа: {numberTwo}");
                }
                else
                {
                    Console.WriteLine($"Число: {numberOne} равно числу: {numberTwo}");
                }
            }

            static void WHILESubroutine(string line, int numberRepetitions)
            {
                Console.WriteLine("Результат: ");
                int i = 0;
                while (i < numberRepetitions)
                {
                    Console.WriteLine(line);
                    i++;
                }
            }

            static void FOREACHSubroutine(string line)
            {
                Console.WriteLine("Результат: ");
                foreach (var item in line)
                {
                    Console.WriteLine(item);
                }
            }

            static void SWITCHSubroutine(int mounthNumber)
            {
                switch (mounthNumber)
                {
                    case 1:
                        Console.WriteLine("Январь");
                        break;
                    case 2:
                        Console.WriteLine("Февраль");
                        break;
                    case 3:
                        Console.WriteLine("Март");
                        break;
                    case 4:
                        Console.WriteLine("Апрель");
                        break;
                    case 5:
                        Console.WriteLine("Май");
                        break;
                    case 6:
                        Console.WriteLine("Июнь");
                        break;
                    case 7:
                        Console.WriteLine("Июль");
                        break;
                    case 8:
                        Console.WriteLine("Август");
                        break;
                    case 9:
                        Console.WriteLine("Сентябрь");
                        break;
                    case 10:
                        Console.WriteLine("Октябрь");
                        break;
                    case 11:
                        Console.WriteLine("Ноябрь");
                        break;
                    case 12:
                        Console.WriteLine("Декабрь");
                        break;
                    default:
                        Console.WriteLine("Такого месяца нет!");
                        break;
                }
            }

            static void DOWHILESubroutine()
            {
                int number;
                do
                {
                    Console.Write("Введите цисло: ");
                    number = Convert.ToInt32(Console.ReadLine());
                    Console.WriteLine($"Вы ввели: {number}");
                }
                while (number != 0);
                Console.WriteLine("Вы ввели 0 и вышли из цикла!");
            }

            MyMethod();
        }
    }
}
