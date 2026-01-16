using calendar.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace calendar.ViewModels.Converters
{
    public class RepeatRuleConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is EntryModel.Repeat repeat)
            {
                switch (repeat)
                {
                case EntryModel.Repeat.Never:
                    return "Nikdy";
                case EntryModel.Repeat.Daily:
                    return "Denně";
                case EntryModel.Repeat.Weekly:
                    return "Týdně";
                case EntryModel.Repeat.Monthly:
                    return "Měsíčně";
                }
            }

            return new string[] { "Nikdy", "Denně", "Týdně", "Měsíčně" };
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            string repeatString = (string)value;

            switch (repeatString)
            {
                case "Nikdy":
                    return EntryModel.Repeat.Never;
                case "Denně":
                    return EntryModel.Repeat.Daily;
                case "Týdně":
                    return EntryModel.Repeat.Weekly;
                case "Měsíčně":
                    return EntryModel.Repeat.Monthly;
            }

            return EntryModel.Repeat.Never;
        }
    }
}