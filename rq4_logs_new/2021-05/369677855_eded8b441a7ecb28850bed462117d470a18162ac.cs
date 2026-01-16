using System;
using AtmosAQ.Domain.Enums;

namespace AtmosAQ.Infrastructure.Helpers
{
    public static class ParseHelper
    {
        public static string ParseEnumToString(GroupTimeEnum groupTimeEnum)
        {
            return groupTimeEnum switch
            {
                GroupTimeEnum.Day => "day",
                GroupTimeEnum.Month => "month",
                GroupTimeEnum.Year => "year",
                _ => string.Empty
            };
        }

        public static string ParseDateToString(DateTime date)
        {
            return date.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss");
        }
    }
}