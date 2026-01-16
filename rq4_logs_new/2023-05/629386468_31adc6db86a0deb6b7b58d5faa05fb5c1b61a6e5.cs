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
    /// Interaction logic for Lesson_Selection.xaml
    /// </summary>
    public partial class Lesson_Selection : Window
    {
        public Lesson_Selection()
        {
            var authorizePersons = Program.AuthorizePersonsDetecter();
            InitializeComponent();
            Profesore.Text = authorizePersons[Program.index].Name + " " + authorizePersons[Program.index].Family;
        }

        private void Select_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                Program.ProfessorSelector(lesson1.Text, lesson2.Text, lesson3.Text);
            }
            catch
            {
                
            }
        }
    }
}