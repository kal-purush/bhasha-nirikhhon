using LanguageSchoolApp.model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;

namespace LanguageSchoolApp.converter
{
    public class ExamPartValueConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is Dictionary<ExamPart, int> scores && parameter is string examPartName)
            {
                if (Enum.TryParse<ExamPart>(examPartName, out var examPart) && scores.ContainsKey(examPart))
                    return scores[examPart];
            }
            return 0;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}