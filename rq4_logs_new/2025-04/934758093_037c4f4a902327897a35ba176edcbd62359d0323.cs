using LanguageSchoolApp.service.Users.Students;
using LanguageSchoolApp.model.Users;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using System.Windows.Data;
using System.Windows.Markup;

namespace LanguageSchoolApp.converter
{
    public class StudentIdToNameConverter : IValueConverter
    {
        private IStudentService _studentService;
        private IStudentService StudentService => _studentService ??= App.ServiceProvider.GetService<IStudentService>(); 
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            string studentId = value as string;
            if (string.IsNullOrEmpty(studentId)) 
            {
                return "";
            }

            Student student = StudentService.GetStudent(studentId);
            return student.Name + " " + student.Surname;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}