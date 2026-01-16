using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace Standard.Tool.Platform.Common.Converters
{
    public class DateTimeConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            string sRet = "";
            try
            {
                sRet = DateTime.ParseExact((string)value, "yyyyMMdd", null).ToString("yyyy-MM-dd");
            }
            catch
            {

            }

            return sRet;
        }

        public object ConvertBack(object value, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            string sRet = "";
            try
            {
                sRet = DateTime.ParseExact((string)value, "yyyy-MM-dd", null).ToString("yyyyMMdd");
            }
            catch
            {

            }

            return sRet;
        }
    }
}