using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

namespace Employees
{
    /// <summary>
    /// Interaction logic for Departments.xaml
    /// </summary>
    public partial class Departments : Window
    {
        public Departments()
        {
            InitializeComponent();
        }

        private void lbDepartments_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {

        }

        private void btnAdd_Click(object sender, RoutedEventArgs e)
        {

        }

        private void btnDelete_Click(object sender, RoutedEventArgs e)
        {
            // удаление выделенного отдела
            var deps = (ObservableCollection<Dep>)lbDepartments.ItemsSource;
            deps.Remove((Dep)lbDepartments.SelectedItem);

        }
    }
}