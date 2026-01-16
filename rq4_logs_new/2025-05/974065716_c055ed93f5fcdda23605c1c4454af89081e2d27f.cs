using System;
using System.Globalization;
using System.Windows.Data;

namespace EasySave.Converters;

[ValueConversion(typeof(DateTime), typeof(string))]
class DateTimeToStringConverter : IValueConverter {
    public object Convert(object value, Type targetType, object parameter, CultureInfo culture) {
        if (value is DateTime dateTime) {
            return dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " (" + (DateTime.Now - dateTime).ToString(@"hh\:mm\:ss") + ")";
        }
        return string.Empty;
    }

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) {
        throw new NotImplementedException();
    }
}