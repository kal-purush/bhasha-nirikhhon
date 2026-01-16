using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace FZChat.Client.ViewModel.Converter
{
    [ValueConversion(typeof(string), typeof(int))]
    public class AgeOptionConverterUltra : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if ((int)value == -1)
            {
                return "不限";
            }
            else
            {
                return ((int)value).ToString();
            }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if ((string)value == "不限")
            {
                return -1;
            }
            else if (value == null)
            {
                return null;
            }
            else
            {
                return int.Parse((string)value);
            }
        }
    }
}