using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace hogward.Windows.Professor
{
    /// <summary>
    /// Interaction logic for ProfessorHomeWork.xaml
    /// </summary>
    public partial class ProfessorHomeWork : Window
    {
        public ProfessorHomeWork()
        {
            InitializeComponent();
            ListLesson.ItemsSource = Program.ProfessorLessonFinder();
        }

        private void Send_Click(object sender, RoutedEventArgs e)
        {
            Program.ProfessorHomeWorkWriter(DeadLine.Text, ListLesson.Text, Title.Text, Convert.ToInt16(Point.Text));
            this.Close();
        }
    }
}