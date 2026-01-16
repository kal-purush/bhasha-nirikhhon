using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;
using System.Windows.Media;

namespace GW2Trader.Util
{
    public class RarityToBrushConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            SolidColorBrush brush = new SolidColorBrush();
            string hexCode;
            switch (value.ToString())
            {
                case "Junk": return Brushes.LightGray;
                    break;
                case "Basic": return Brushes.LightGray;
                    break;
                case "Fine": hexCode = "#62a4da";
                    break;
                case "Masterwork": hexCode = "#02d439";
                    break;
                case "Rare": hexCode = "#fcd00b";
                    break;
                case "Exotic": hexCode = "#ffa405";
                    break;
                case "Ascended": hexCode = "#fb3e8d";
                    break;
                case "Legendary": hexCode ="#4c139d";
                    break;
                default: return Brushes.White;
            }
            return (SolidColorBrush)(new BrushConverter().ConvertFrom(hexCode));
        }

        public object ConvertBack(object value, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}