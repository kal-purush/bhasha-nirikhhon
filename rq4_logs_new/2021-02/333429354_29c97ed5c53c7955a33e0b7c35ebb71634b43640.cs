using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TMS.Net07.Homework.DaysOfWeek.MiddleLevel
{
    class Program
    {
        static string GetDayOfWeekMiddleLevel(string date)
        {
            if (date == "exit")
            {
                var exit = "You closed the program!";
                return exit;
            }
            try
            {
                DateTime objdate = Convert.ToDateTime(date);
                return objdate.DayOfWeek.ToString();
            }
            catch (Exception)
            {
                Console.WriteLine("Bad input! ");
            }
            return date;
        }
        static string GetDayOfWeekHardLevel(string date)
        {
            string[] num = date.Split('.');

            var day = Convert.ToInt32(num[0]);
            var month = Convert.ToInt32(num[1]);
            var year = Convert.ToInt32(num[2]);

            Console.WriteLine(num[0]);
            Console.WriteLine(num[1]);
            Console.WriteLine(num[2]);

            if (month == 06)
            {
                if (day == 1) { Console.WriteLine("Monday"); }
                else if (day == 2) { Console.WriteLine("Tuesday"); }
                else if (day == 3) { Console.WriteLine("Wednsday"); }
                else if (day == 4) { Console.WriteLine("Thursday"); }
                else if (day == 5) { Console.WriteLine("Friday"); }
                else if (day == 6) { Console.WriteLine("Saturday"); }
                else if (day == 7) { Console.WriteLine("Sunday"); }
            }
            Console.WriteLine("\n");
            return date;
        }
        static void Main(string[] args)
        {
            Console.Write("Input date: ");
            var date = Console.ReadLine();

            var dayOfWeekFromMiddleLevel = GetDayOfWeekMiddleLevel(date);
            var dayOfWeekTimeFromHardLevel = GetDayOfWeekHardLevel(date);

            Console.WriteLine($"Result from middle level: {dayOfWeekFromMiddleLevel}");
            Console.WriteLine($"Result from hard level: {dayOfWeekTimeFromHardLevel}");
        }
    }
}