using LanguageSchoolApp.viewModel.Exams;
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
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace LanguageSchoolApp.view.Exams
{
    /// <summary>
    /// Interaction logic for GradeExamsView.xaml
    /// </summary>
    public partial class GradeExamsView : UserControl
    {
        public GradeExamsView()
        {
            InitializeComponent();
        }

        //private void TextBox_LostFocus(object sender, RoutedEventArgs e)
        //{
        //    if (DataContext is GradeExamsViewModel viewModel) 
        //    { 
        //        viewModel.CalculateTotalScore();
        //    }
        //}
    }
}