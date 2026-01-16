using CSB.Business.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace CSB.GUI
{
    /// <summary>
    /// Interaction logic for DeleteEmployee.xaml
    /// </summary>
    public partial class DeleteEmployee : Window
    {
        private readonly IEmployeeService _employeeService;
        private readonly IServiceProvider _serviceProvider;


        public DeleteEmployee(IEmployeeService employeeService, IServiceProvider serviceProvider)
        {
            InitializeComponent();
            _employeeService = employeeService;
            this._serviceProvider = serviceProvider;
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {

            Close();
        }

        private void Delete_Click(object sender, RoutedEventArgs e)
        {
            int id = Convert.ToInt32(IDinput.Text);

            if (IDinput.Text.Any())
            {
                _employeeService.DeleteAsync(id);
                Close();
            }
            
        }

        private void IDinput_TextChanged(object sender, System.Windows.Input.TextCompositionEventArgs e)
        {
            Regex regex = new Regex("[^0-9]+");
            e.Handled = regex.IsMatch(e.Text);
        }
    }
}