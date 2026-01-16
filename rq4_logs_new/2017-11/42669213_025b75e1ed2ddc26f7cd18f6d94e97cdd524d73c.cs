using System;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Data;

namespace CTime2.Converter
{
    public class ZeroToVisibilityCollapsedConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, string language)
        {
            var number = (double)value;

            return number == 0
                ? Visibility.Collapsed
                : Visibility.Visible;
        }

        public object ConvertBack(object value, Type targetType, object parameter, string language)
        {
            throw new NotSupportedException();
        }
    }
}