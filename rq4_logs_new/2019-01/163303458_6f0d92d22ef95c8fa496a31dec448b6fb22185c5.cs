using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Base_Lesson_9.Extentions
{
    public static class DateTimeExtention
    {
        public static void Deconstruct(this DateTime p, out int year, out int month, out int day, out int hours, out int minutes, out int seconds)
        {
            year = p.Year;
            month = p.Month;
            day = p.Day;
            hours = p.Hour;
            minutes = p.Minute;
            seconds = p.Second;
        }
    }
}