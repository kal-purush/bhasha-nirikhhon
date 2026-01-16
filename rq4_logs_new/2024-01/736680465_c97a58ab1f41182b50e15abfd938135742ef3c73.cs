using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Transactions;

namespace ConsoleApp1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            static void MyMethod()
            {
                while (true)
                {
                    try
                    {


                        int choise;
                        Console.WriteLine("Для вызова выполняемой подпрограммы укажите ее номер и нажните Enter:" +
                            "\r\n1 - IF ELSE\r\n2 - WHILE\r\n3 - DO WHILE\r\n4 - FOR\r\n5 - FOREACH\r\n6 - SWITCH");
                        choise = Convert.ToInt32(Console.ReadLine());


                        switch (choise)


                        {
                            case 1:
                                Console.WriteLine("\n");
                                Console.WriteLine("Пример IF ELSE");
                                char logicOperation;
                                Console.WriteLine("у нас есть числа 10 и 5, какую логическую операцию выполнить? + или -");
                                logicOperation = Convert.ToChar(Console.ReadLine());
                                if (logicOperation == '+')
                                    Console.WriteLine($"Ответ: {10 + 5}");
                                else if (logicOperation == '-')
                                    Console.WriteLine($"Ответ: {10 - 5}");
                                else
                                    Console.WriteLine("Некорректный ввод");
                                ExitMenu();
                                Console.Clear();

                                break;

                            case 2:
                                Console.WriteLine("\n");
                                Console.WriteLine("Пример цикла While");
                                string word;
                                int repeat;
                                Console.Write("Укажите слово: ");
                                word = Console.ReadLine();
                                try
                                {

                                    Console.WriteLine("Сколько раз его повторить? ");
                                    repeat = Convert.ToInt32(Console.ReadLine());
                                    int i = 0;
                                    while (i < repeat)
                                    {
                                        Console.WriteLine(word);
                                        i++;
                                    }

                                }
                                catch
                                {
                                    Console.WriteLine("Некорректный ввод");

                                }
                                ExitMenu();
                                Console.Clear();
                                break;

                            case 3:
                                Console.WriteLine("\n");
                                Console.WriteLine("Пример цикла DO While");
                                int num;
                                try
                                {


                                    do
                                    {
                                        Console.WriteLine("Чтобы выйти из цикла нажмите: 0");
                                        Console.Write("Введите цисло: ");
                                        num = Convert.ToInt32(Console.ReadLine());
                                        Console.WriteLine($"Вы ввели: {num}");
                                    }
                                    while (num != 0);
                                    Console.WriteLine("Вы ввели 0 и вышли из цикла!");
                                }
                                catch
                                {
                                    Console.WriteLine("Некорректный ввод");
                                }
                                ExitMenu();
                                Console.Clear();
                                break;

                            case 4:
                                Console.WriteLine("\n");
                                Console.WriteLine("Пример цикла For");
                                int number, number2;
                                try
                                {
                                    Console.WriteLine("Напиши диапазон чисел");
                                    Console.Write("от: ");
                                    number = Convert.ToInt32(Console.ReadLine());
                                    Console.Write("до: ");
                                    number2 = Convert.ToInt32(Console.ReadLine());

                                    if (number <= number2)
                                    {
                                        for (int x = number; x <= number2; x++)
                                        {

                                            Console.WriteLine(x);


                                        }

                                    }
                                    else
                                        Console.WriteLine("Некорректно ");
                                    ExitMenu();
                                }
                                catch
                                {
                                    Console.WriteLine("Некорректный вод, нужно написать обязательно числа");

                                }
                                ExitMenu();
                                Console.Clear();
                                break;
                            case 5:
                                Console.WriteLine("\n");
                                Console.WriteLine("Пример цикла FOREACH");
                                string names;
                                Console.Write("Напиши свое имя, а я его напишу по буквам: ");
                                names = Console.ReadLine();

                                foreach (char item in names)
                                {
                                    Console.WriteLine(item);
                                }
                                ExitMenu();
                                Console.Clear();
                                break;
                            case 6:
                                try
                                {
                                    Console.WriteLine("\n");
                                    Console.WriteLine("Конструкция switch");
                                    Console.WriteLine("Напишите номер дня недели: ");
                                    int days;
                                    days = Convert.ToInt32(Console.ReadLine());
                                    switch (days)
                                    {
                                        case 1:
                                            Console.WriteLine("Понедельник");
                                            break;
                                        case 2:
                                            Console.WriteLine("Вторник");
                                            break;
                                        case 3:
                                            Console.WriteLine("Среда");
                                            break;
                                        case 4:
                                            Console.WriteLine("Четверг");
                                            break;
                                        case 5:
                                            Console.WriteLine("Пятница");
                                            break;
                                        case 6:
                                            Console.WriteLine("Суббота");
                                            break;
                                        case 7:
                                            Console.WriteLine("Воскресенье");

                                            break;
                                        default:
                                            Console.WriteLine("Нет такого дня");
                                            break;
                                    }

                                }
                                catch
                                {
                                    Console.WriteLine("Некорректный ввод, принимаю только числа");


                                }
                                ExitMenu();
                                Console.Clear();

                                break;
                            default:
                                Console.WriteLine("Неверный ввод");
                                break;
                        }
                    }
                    catch
                    {
                        Console.Clear();

                    }
                }



            }
            MyMethod();
            static void ExitMenu()
            {

                while (true)
                {
                    Console.WriteLine("Для выхода в меню нажмите 'ESC'");

                    ConsoleKeyInfo keyInfo = Console.ReadKey();
                    if (keyInfo.Key == ConsoleKey.Escape)
                        break;
                    else
                        continue;
                }


            }
            static void Week()
            {
                Console.WriteLine("\n");
                Console.WriteLine("Конструкция switch");
                Console.WriteLine("Напишите номер дня недели: ");
                int days;
                days = Convert.ToInt32(Console.ReadLine());
                switch (days)
                {
                    case 1:
                        Console.WriteLine("Понедельник");
                        break;
                    case 2:
                        Console.WriteLine("Вторник");
                        break;
                    case 3:
                        Console.WriteLine("Среда");
                        break;
                    case 4:
                        Console.WriteLine("Четверг");
                        break;
                    case 5:
                        Console.WriteLine("Пятница");
                        break;
                    case 6:
                        Console.WriteLine("Суббота");
                        break;
                    case 7:
                        Console.WriteLine("Воскресенье");

                        break;
                    default:
                        Console.WriteLine("Нет такого дня");
                        break;
                }



            }

        }
    }
}