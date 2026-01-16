using LanguageSchoolApp.service.Courses;
using LanguageSchoolApp.model.Courses;
using Microsoft.Extensions.DependencyInjection;
using System.Globalization;
using System.Windows.Data;

namespace LanguageSchoolApp.converter
{
    public class CourseIdToCourseProficiencyConverter : IValueConverter
    {
        private ICourseService _courseService;
        private ICourseService CourseService => _courseService ??= App.ServiceProvider.GetService<ICourseService>();
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            int courseId = (int)value;
            if (!CourseService.CourseExists(courseId)) 
            {
                return "";
            }
            Course course = CourseService.GetCourse(courseId);
            return course.LanguageProficiency.LanguageName + " " + course.LanguageProficiency.LanguageLevel.ToString();
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}