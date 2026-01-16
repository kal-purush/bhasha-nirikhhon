using System;
using UtilityLibraries;

namespace BodyMassIndex
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            // Ввести вес и рост человека.Рассчитать и вывести индекс массы тела(ИМТ) по формуле I = m / (h * h);
            // где m — масса тела в килограммах, h — рост в метрах

            Console.WriteLine("Программа рассчета индекса массы тела.");

            double PersonHeight = 0;
            double PersonWeight = 0;

            while (PersonHeight <= 0 || PersonWeight <= 0)
            {
                try
                {
                    if (PersonHeight <= 0)
                    {
                        ConsoleHelper.ReadAndAssignInput("Введите рост, м", ref PersonHeight);
                    }
                    if (PersonWeight <= 0)
                    {
                        ConsoleHelper.ReadAndAssignInput("Введите массу тела, кг", ref PersonWeight);
                    }
                }
                catch (Exception exception)
                {
                    Console.WriteLine("Ошибка ввода! " + exception.Message);
                }
            }

            Console.WriteLine("ИМТ: " + CalculateBMI(PersonWeight, PersonHeight));
            Console.ReadKey();
        }

        private static double CalculateBMI(double PersonWeight, double PersonHeight)
        {
            return PersonWeight / (PersonHeight * PersonHeight);
        }
    }
}