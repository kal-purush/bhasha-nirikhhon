using System;

namespace Homework4
{
    class MainClass
    {


        public static void Main()

        {

            while (true)
            {
                Console.WriteLine("=========== Домашняя работа №4 ==========\n");
                Console.WriteLine("1 - Задача 1");
                Console.WriteLine("2 - Задача 2\n");
                Console.WriteLine("0 - Заверешние работы приложения\n");
                Console.WriteLine("=========================================");
                Console.Write("Введите номер задачи: ");

                int taskNum = int.Parse(Console.ReadLine());

                switch (taskNum)
                {
                    case 0:
                        return;
                    case 1:
                        (string name, string lastName, string father)[] user = CreateUsers();
                        ShowMenu(user);
                        break;
                    case 2:
                        Console.Write("Введите числа через пробел: ");
                        string numbersWrite = Console.ReadLine();
                        

                        int sumNumbers = sumNum(numbersWrite);
                        Console.WriteLine($"Сумма введенных вами чисел: {sumNumbers}");
                        break;
                    default:
                        Console.WriteLine("Вы ввели некорректное число");
                        break;
                }
                
            }
        }

        /// <summary>
        /// Задание №1 Написать метод принимающий на вход ФИО
        /// в разных аргументах и возвращающий объединённую строку с ФИО
        /// Используя метод, написать программу, выводящую в консоль 3–4 разных ФИО.
        /// </summary>
        /// <returns></returns>

        static (string name, string lastName, string patronymic)[] CreateUsers()
        {
            Console.Write("Введите количество пользователей: ");
            int count = int.Parse(Console.ReadLine());
            (string name, string lastName, string patronymic)[] users = new (string name, string lastName, string patronymic)[count];
            for (int i = 0; i < users.Length; i++)
            {
                users[i] = CreateUser();
                Console.WriteLine($"Пользователь {FormatUserData(users[i])} сохранен");
            }
            Console.WriteLine("Ввод данных завершён. Нажмите любую клавишу...");
            Console.ReadKey();
            Console.Clear();
            return users;
        }


        static void ShowMenu((string name, string lastname, string patronymic)[] users)
        {
            int choice;
            do
            {
                choice = GetUserChoice();
                switch (choice)
                {
                    case 0: return;
                    case 1:
                        PrintUsers(users);
                        break;
                    case 2:
                        PrintSelectedUser(users);
                        break;
                    default:
                        break;
                }
            } while (choice != 0);
        }



        static (string userName, string lastName, string patronymic) CreateUser()
        {
            Console.Write("Введите ваше имя: ");
            string name = Console.ReadLine();
            Console.Write("Введите вашу фамилию: ");
            string lastName = Console.ReadLine();
            Console.Write("Введите ваше отчество: ");
            string patronymic = Console.ReadLine();
            return (lastName ,name,patronymic);

        }


        static int GetUserChoice()
        {
            Console.WriteLine("===================================");
            Console.WriteLine("1 - Просмотр всей базы данных");
            Console.WriteLine("2 - Просмотр пользователя\n");
            Console.WriteLine("0 - Для выхода в основное меню");
            Console.WriteLine("===================================");
            Console.Write("Введите номер задачи: ");
            return int.Parse(Console.ReadLine());
        }



        static void PrintSelectedUser((string name, string lastName, string patronymic)[] users)
        {
            int userIndex;
            do
            {
                Console.WriteLine($"Введите идентификатор пользователя - от 0 до {users.Length - 1}");
                userIndex = int.Parse(Console.ReadLine());
                
            } while (userIndex < 0 || userIndex >= users.Length);
            PrintUser(users[userIndex]);
        }


        static void PrintUsers((string name, string lastName, string patronymic)[] users)
        {
            Console.WriteLine("Вывод базы данных:");
            for (int i = 0; i < users.Length; i++)
            {
                PrintUser(users[i]);
            }
        }


        static void PrintUser((string name, string lastName, string patronymic) user)
        {
            Console.WriteLine(FormatUserData(user));
        }


        static string FormatUserData((string name, string lastNane, string patronymic) user)
        {
            return $"{user.lastNane} {user.name} {user.patronymic}";
        }

        /// <summary>
        /// Написать программу, принимающую на вход строку — набор чисел,
        /// разделенных пробелом, и возвращающую число — сумму всех чисел в строке.
        /// Ввести данные с клавиатуры и вывести результат на экран.
        /// </summary>
        static int sumNum(string numbers)
        {
            string[] num = numbers.Split(separators, StringSplitOptions.RemoveEmptyEntries);
            int sum = 0;
            for(int i = 0; i < num.Length; i ++)
            {
                sum += int.Parse(num[i]);
            }

            return sum;
        }

        static string[] separators = { " ", ",", "!", "?", ":", ";", "." };
    }
}