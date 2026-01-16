using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace SuperRecall.Views.Converters
{
    class ReviseCountConverter : IMultiValueConverter
    {
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            string text = values[0].ToString() + ": ";
            ObservableCollection<DateTime> reviewDates;

            try { reviewDates = (ObservableCollection<DateTime>) values[1]; }
            catch { return ""; }

            return text + reviewDates.Count;
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}