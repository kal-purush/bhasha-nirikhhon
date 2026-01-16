using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Xamarin.Forms;

namespace OpenWeatherMap.Converters
{
    public class DateTimeConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            //DateTime dateTime = new DateTime((long)value);
            DateTimeOffset dateTimeOffset = DateTimeOffset.FromUnixTimeSeconds((long)value);
            //return dateTimeOffset.Hour.ToString("t") + ":" + dateTimeOffset.Minute.ToString("MM");
            return dateTimeOffset.ToString("t");
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}