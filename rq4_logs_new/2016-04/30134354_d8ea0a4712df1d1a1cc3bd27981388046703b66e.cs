using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace LogArchive.Views.Converters
{
	public class StringToIntergerConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			int integer = (int)value;

			return integer == 0 ? "" : integer.ToString();
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			string text = (string)value;
			int integer = 0;

			int.TryParse(text, out integer);

			return integer;
		}
	}
}