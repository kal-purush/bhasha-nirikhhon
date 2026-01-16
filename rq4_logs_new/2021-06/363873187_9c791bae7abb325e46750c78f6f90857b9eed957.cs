using CSB.Business.Interfaces;
using CSB.Business.Models;
using Microsoft.Extensions.DependencyInjection;
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

namespace CSB.GUI
{
    /// <summary>
    /// Interaction logic for AddEmployee.xaml
    /// </summary>
    public partial class AddEmployee : Window
    {
        private readonly IEmployeeService _employeeService;
        private readonly IServiceProvider _serviceProvider;
        

        public AddEmployee(IEmployeeService employeeService, IServiceProvider serviceProvider)
        {
            InitializeComponent();
            _employeeService = employeeService;
            this._serviceProvider = serviceProvider;
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            
            Close();
        }

        private void Add_Click(object sender, RoutedEventArgs e)
        {

            CreateEmployeeDto newEmployee = new CreateEmployeeDto();



            if (FirstNameInput.Text.Any() && LastNameInput.Text.Any() && EmailInput.Text.Any() && BirthdateInput.SelectedDate.HasValue && ((bool)MaleGenderRadio.IsChecked || (bool)FemaleGenderRadio.IsChecked))
            {
                newEmployee.FirstName = FirstNameInput.Text;
                newEmployee.LastName = LastNameInput.Text;
                newEmployee.Email = EmailInput.Text;
                newEmployee.DateOfBirth = (DateTime)BirthdateInput.SelectedDate;

                if ((bool)FemaleGenderRadio.IsChecked)
                {
                    newEmployee.Gender = Business.Enums.Gender.Female;
                }
                if ((bool)MaleGenderRadio.IsChecked)
                {
                    newEmployee.Gender = Business.Enums.Gender.Male;
                }

                _employeeService.CreateAsync(newEmployee);
                Close();
            }
            
        }
    }
}