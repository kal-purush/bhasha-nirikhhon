using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace FactorySADEfficiencyOptimizer.UIComponent.Converters;

[ValueConversion(typeof(int), typeof(string))]
class CompletedDayConverter : IValueConverter
{
    public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
    {
        if (value is null)
            return "not run";
        int? day = (int)value;

        return day.ToString() ?? throw new Exception("Converter failed to handle null value.");
    }

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}