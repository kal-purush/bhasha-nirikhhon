using System;

namespace Maker.Identity.Stores.Extensions
{
    public static class DateTimeExtensions
    {
        public static DateTimeOffset ToDateTimeOffset(this DateTime dateTime, TimeSpan offset)
        {
            return new DateTimeOffset(dateTime, offset);
        }

    }
}