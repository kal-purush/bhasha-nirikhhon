using CSB.Business.Interfaces;
using CSB.GUI.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;

namespace CSB.GUI.Commands
{
    public class GetEmployeesCommand : ICommand
    {
        private readonly IEmployeeService _employeeService;
        private readonly MainEmployeeViewModel _mainEmployeeViewModel;

        public GetEmployeesCommand(
            IEmployeeService employeeService,
            MainEmployeeViewModel mainEmployeeViewModel)
        {
            _employeeService = employeeService;
            _mainEmployeeViewModel = mainEmployeeViewModel;
            CanExecuteChanged?.Invoke(this, EventArgs.Empty);
        }

        public event EventHandler CanExecuteChanged;

        public bool CanExecute(object parameter)
        {
            return true;
        }

        public async void Execute(object parameter)
        {
            var employees = await _employeeService.GetAllAsync();
            _mainEmployeeViewModel.Employees = employees.ToList();
        }
    }
}