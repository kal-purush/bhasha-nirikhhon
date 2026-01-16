using System;
using System.Globalization;

namespace Homework_2
{
    class Program
    {
       static string UserDayOfWeek(string str)
        {
            try
            {
                DateTime UserDayOfWeek = DateTime.ParseExact(str, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);

                int WeekDayNumer = 0;
                switch (UserDayOfWeek.DayOfWeek)
                {
                    case DayOfWeek.Sunday:
                        WeekDayNumer = 7;
                        break;
                    case DayOfWeek.Monday:
                        WeekDayNumer = 1;
                        break;
                    case DayOfWeek.Tuesday:
                        WeekDayNumer = 2;
                        break;
                    case DayOfWeek.Wednesday:
                        WeekDayNumer = 3;
                        break;
                    case DayOfWeek.Thursday:
                        WeekDayNumer = 4;
                        break;
                    case DayOfWeek.Friday:
                        WeekDayNumer = 5;
                        break;
                    case DayOfWeek.Saturday:
                        WeekDayNumer = 6;
                        break;
                }

                Console.WriteLine($"{str} is {UserDayOfWeek.DayOfWeek}, the {WeekDayNumer} day of the week.");
            }
            catch (Exception)
            {
                Console.WriteLine("Unable to convert {0} to a date.", str);
            }

            return "";
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Write date in format: dd/mm/yyyy");
            string UserDate = Console.ReadLine();
            Console.WriteLine(UserDayOfWeek(UserDate));
        }
    }
}