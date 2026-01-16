using System;

namespace Less4
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(GetFullName("Сидоров","Петр","Бесланович"));
            Console.WriteLine(GetFullName("Нишкина", "Анна", "Семенович"));
            Console.WriteLine(GetFullName("Мурзилкин", "Леонид", "Котович"));
            Console.WriteLine(GetFullName("Столяров", "Вася", "Васильевич"));
            Console.ReadKey();
        }

        static string GetFullName(string firstName, string lastName, string patronymic) 
        {
            return firstName + " " + lastName + " " + patronymic;
        }

    }
}