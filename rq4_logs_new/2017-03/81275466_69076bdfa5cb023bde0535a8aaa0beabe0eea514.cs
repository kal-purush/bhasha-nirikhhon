using System;
using Xamarin.Forms.Xaml;
using Xamarin.Forms;
using System.Globalization;


namespace MyCalisthenics
{
	public class DateFormatConverter : IValueConverter
	{
		public DateFormatConverter()
		{
		}

		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var date = (DateTime)value;
			return date.ToString("dddd, MMM dd");
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			return (DateTime)value;
		}
	}
}