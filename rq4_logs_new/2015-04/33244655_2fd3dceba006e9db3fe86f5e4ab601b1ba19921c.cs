using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;
using System.Windows.Media;

namespace Adam_module
{

    class StateToColorConverter: IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            bool status = (bool)value;
            if (status==true)
            {  
                return new SolidColorBrush(Colors.Green); 
            }
            else 
            {
                return new SolidColorBrush(Colors.Red); 
            }
              
         
                
        }

        public object ConvertBack(object value, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}