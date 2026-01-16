using System;
using System.Globalization;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;

namespace DigitalDisplay.Wpf.Converters
{
    /// <summary>
    /// Represents the converter that converts <see cref="uint"/> values to 
    /// <see cref="ControlTemplate"/> values.
    /// </summary>
    public class ValueToDigitTemplateConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            uint val = (uint)value;
            if (val > 9)
            {
                val = 0;
            }
            string template = string.Format("Digit{0}", val);
            return Application.Current.Resources[template];
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return Binding.DoNothing;
        }
    }
}