using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NicoV2.Common
{
    public static class Converter
    {
        public static DateTime ToDatetime(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return default(DateTime);
            }
            else
            {
                return DateTime.Parse(value);
            }
        }
    }
}