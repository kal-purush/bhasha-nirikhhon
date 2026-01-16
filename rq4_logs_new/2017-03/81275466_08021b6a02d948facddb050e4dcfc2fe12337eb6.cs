using System;
using System.Globalization;
using Xamarin.Forms;
namespace MyCalisthenics
{
	public class IntstringConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			//var stringval = (string)value;
			return value;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}