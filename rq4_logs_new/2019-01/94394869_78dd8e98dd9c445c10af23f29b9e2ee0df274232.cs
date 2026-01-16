using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;
using Xamarin.Forms;
using YanSoft.CurrencyExchanger.Core.Utils;

namespace YanSoft.CurrencyExchanger.Core.Converters
{
    public class FlagConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var filePath = string.Empty;
            if (value != null)
            {
                filePath = PlatformHelper.GetImagePath($"{value.ToString()}.png");
            }
            else
            {
                filePath = PlatformHelper.GetImagePath($"flag_white.png");
            }
            // Or you can return a ImageSource object here.
            //var resource = ImageSource.FromFile(filePath);
            return filePath;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}