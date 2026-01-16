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

namespace Employees
{
    /// <summary>
    /// Interaction logic for Employee.xaml
    /// </summary>
    public partial class Employee : Window
    {
        private Empl _emp;
        public Empl Emp
        { 
            get
            {
                _emp.Name = txtName.Text;
                _emp.Age = Int32.Parse(txtAge.Text);
                _emp.Salary = Double.Parse(txtSalary.Text);
                return _emp;
            }
            set
            {
                _emp = value;
                txtId.Text = _emp.Id.ToString();
                txtName.Text = _emp.Name;
                txtAge.Text = _emp.Age.ToString();
                txtSalary.Text = _emp.Salary.ToString();
            }
        }

        public Employee()
        {
            InitializeComponent();
        }

        private void btnOk_Click(object sender, RoutedEventArgs e)
        {

        }

        private void btnCancel_Click(object sender, RoutedEventArgs e)
        {

        }
    }
}