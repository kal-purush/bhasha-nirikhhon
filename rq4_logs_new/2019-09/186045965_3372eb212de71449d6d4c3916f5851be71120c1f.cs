using System;

namespace Singer.Helpers.Extensions
{
   public static class DateTimeExtensions
   {
      public static DateTime SetTime(this DateTime date, DateTime time)
      {
         return new DateTime(date.Year, date.Month, date.Day, time.Hour, time.Minute, time.Second, time.Millisecond, date.Kind);
      }
   }
}