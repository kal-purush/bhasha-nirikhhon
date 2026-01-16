using System;

namespace HomeworkOleg.General
{
    class Homework1_Variables
    {
        public static void Start()
        {
            Console.WriteLine("Как меня зовут?");
            string name = Console.ReadLine();

            Console.WriteLine("Сколько мне лет?");
            string old = Console.ReadLine();

            Console.WriteLine("Где Я живу?");
            string address = Console.ReadLine();

            Console.WriteLine("Кем Я работаю?");
            string profession = Console.ReadLine();

            Console.WriteLine("Как зовут Мою девушку?");
            string gilrName = Console.ReadLine();

            Console.WriteLine("Мое имя  " + name);
            Console.WriteLine("Возраст  " + old);
            Console.WriteLine("Адрес  " + address);
            Console.WriteLine("Профессия  " + profession);
            Console.WriteLine("Девушку зовут  " + gilrName);
        }
    }
}