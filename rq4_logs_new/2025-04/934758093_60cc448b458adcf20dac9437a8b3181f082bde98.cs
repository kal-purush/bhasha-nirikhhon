using LanguageSchoolApp.service.Courses;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using System.Windows.Data;
using LanguageSchoolApp;

namespace LanguageSchoolApp.converter
{
    public class StudentAppliedToStringConverter : IMultiValueConverter
    {
        private ICourseApplicationService _courseApplicationService;
        private ICourseApplicationService CourseApplicationService => _courseApplicationService ??= App.ServiceProvider.GetService<ICourseApplicationService>();

        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            if (values.Length < 2 || values[0] == null || values[1] == null)
            {
                return string.Empty;
            }
            string studentId = values[0].ToString();
            int courseId;
            if (values[1] is int)
            {
                courseId = (int)values[1];
            }
            else if (!int.TryParse(values[1].ToString(), out courseId))
            {
                return string.Empty;
            }
            int CourseApplicationId = CourseApplicationService.GenerateId(studentId, courseId);
            return CourseApplicationService.CourseApplicationExists(CourseApplicationId) ? "Withdraw" : "Apply";
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}