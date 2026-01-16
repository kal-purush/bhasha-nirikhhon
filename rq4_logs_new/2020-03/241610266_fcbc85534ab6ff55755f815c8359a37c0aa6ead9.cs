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
    /// Interaction logic for Department.xaml
    /// </summary>
    public partial class Department : Window
    {
        private Dep _dep;
        public Dep Dep
        {
            get
            {
                return _dep;
            }
            set
            {
                _dep = value;
                txtId.Text = _dep.Id.ToString();
                txtName.Text = _dep.Name;
            }
        }



        public Department()
        {
            InitializeComponent();
        }

        private void btnOk_Click(object sender, RoutedEventArgs e)
        {
            _dep.Name = txtName.Text;

            this.DialogResult = true;
        }
    }
}