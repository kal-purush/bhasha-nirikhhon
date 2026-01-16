using System;
using System.Collections.Generic;
using System.Text;

namespace YanSoft.CurrencyExchanger.Core.Utils
{
    public static class DateTimeHelper
    {
        /// <summary>
        /// method for converting a UNIX timestamp to a regular
        /// System.DateTime value (and also to the current local time)
        /// </summary>
        /// <param name="timestamp">value to be converted</param>
        /// <returns>converted DateTime in string format</returns>
        public static DateTime ConvertTimestampToDateTime(double timestamp)
        {
            //create a new DateTime value based on the Unix Epoch
            var converted = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            //add the timestamp to the value
            DateTime newDateTime = converted.AddSeconds(timestamp);

            //return the value in string format
            return newDateTime.ToUniversalTime();
        }
        /// <summary>
        /// method for converting a System.DateTime value to a UNIX Timestamp
        /// </summary>
        /// <param name="value">date to convert</param>
        /// <returns></returns>
        public static int ConvertDateTimeToTimestamp(DateTime value)
        {
            //create Timespan by subtracting the value provided from
            //the Unix Epoch
            //TimeSpan span = (value - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToUniversalTime());
            TimeSpan span = (value.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));

            //return the total seconds (which is a UNIX timestamp)
            return (int)span.TotalSeconds;
        }

        public static long ConvertDateTimeToTimestampByMilliseconds(DateTime value)
        {
            TimeSpan span = (value.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToUniversalTime());

            //return the total seconds (which is a UNIX timestamp)
            return (long)span.TotalMilliseconds;
        }
    }
}