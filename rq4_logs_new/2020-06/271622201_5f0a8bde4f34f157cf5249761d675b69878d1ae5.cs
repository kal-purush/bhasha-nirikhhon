using System;

namespace Homework_1
{
    enum Days
    {
        Monday = 1,
        Tuesday = 2,
        Wednesday = 3,
        Thursday = 4,
        Friday = 5,
        Saturday = 6,
        Sunday = 7
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Hello, please enter the day number of the week, \n" +
                "or day name (Mo, Tu, We etc) : ");
            string DayOfWeek = Console.ReadLine();

            bool result = int.TryParse(DayOfWeek, out int NumerOfWeek);

            if (DayOfWeek == "Mo" || NumerOfWeek == 1)
            {
                Console.WriteLine("Monday is the 1 day of week.");
            }
            else if (DayOfWeek == "Tu" || NumerOfWeek == 2)
            {
                Console.WriteLine("Tuesday is the 2 day of week.");
            }
            else if (DayOfWeek == "We" || NumerOfWeek == 3)
            {
                Console.WriteLine("Wednesday is the 3 day of week.");
            }
            else if (DayOfWeek == "Th" || NumerOfWeek == 4)
            {
                Console.WriteLine("Thursday is the 4 day of week.");
            }
            else if (DayOfWeek == "Fr" || NumerOfWeek == 5)
            {
                Console.WriteLine("Friday is the 5 day of week.");
            }
            else if (DayOfWeek == "Sa" || NumerOfWeek == 6)
            {
                Console.WriteLine("Saturday is the 6 day of week.");
            }
            else if (DayOfWeek == "Su" || NumerOfWeek == 7)
            {
                Console.WriteLine("Sunday is the 7 day of week.");
            }
            else
            {
                Console.WriteLine("You entered something wrong.");
            }
        }
    }
}