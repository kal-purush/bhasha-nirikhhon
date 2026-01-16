using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace Snake.Converters
{
    class PlaceImageConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            string place = (string)value;

            if (place == "01")
            {
                return "/Images/BestResults/1stPlace.png";
            }
            else if (place == "02")
            {
                return "/Images/BestResults/2ndPlace.png";
            }
            else if (place == "03")
            {
                return "/Images/BestResults/3rdPlace.png";
            }
            else return null;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}