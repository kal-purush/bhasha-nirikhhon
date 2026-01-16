using LanguageSchoolApp.model.Courses;
using LanguageSchoolApp.viewModel.Courses;
using System.Windows;

namespace LanguageSchoolApp.view.Courses
{
    /// <summary>
    /// Interaction logic for CourseSchedule.xaml
    /// </summary>
    public partial class CourseSchedule : Window
    {
        public CourseSchedule(List<ClassPeriod> allClasses)
        {
            InitializeComponent();
            DataContext = new CourseScheduleViewModel(allClasses);
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }
    }
}