using System;

namespace Common.Common
{
    /// <summary>
    /// Tostring conversion for datepicker passed to datetime
    /// </summary>
    public class DateConversion
    {
        public string ConvertedDate(DateTime date)
        {
            string a = date.ToString("yyyyMMdd");
            return a;
        }
    }
}