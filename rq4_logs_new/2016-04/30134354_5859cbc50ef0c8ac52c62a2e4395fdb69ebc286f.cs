using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace LogArchive.Views.Converters
{
	public class DateTimeToStringConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			DateTime time = (DateTime)value;

			return $"{time.Year}-{time.Month}-{time.Day}";
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			string text = (string)value;
			string[] dateText = text.Split('-');

			int year = 0;
			int month = 0;
			int day = 0;
			if (dateText.Count() == 3 && int.TryParse(dateText[0], out year) && int.TryParse(dateText[1], out month) && int.TryParse(dateText[2], out day))
			{
				return new DateTime(year, month, day);
			}

			return DateTime.Now;
		}
	}
}