using Base64Universal.Enums;
using System;
using System.Collections.Generic;
using System.Text;
using Windows.UI.Xaml.Data;

namespace Base64Universal.Converters
{
    public class BaseNumberEnumConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, string language)
        {
            return value.ToString() == parameter.ToString();
            // the above code equals this:
            //if (value.ToString() == parameter.ToString())
            //{
            //    return true;
            //}
            //return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, string language)
        {
            return (bool)value ? Enum.Parse(typeof(CodingNumberBaseEnum), parameter.ToString(), true) : null;
            //the above code equals this:
            //if ((bool)value)
            //    return Enum.Parse(typeof(CodingBaseEnum), parameter.ToString(), true);
            //return null;
        }
    }
}