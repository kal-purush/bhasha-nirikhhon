using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class EditUserCommand : ICommand
    {
        public EditUserCommand(UserViewModel viewModel)
        {
            _ViewModel = viewModel;
        }
        private UserViewModel _ViewModel;

        public event EventHandler CanExecuteChanged
        {
            add { CommandManager.RequerySuggested += value; }
            remove { CommandManager.RequerySuggested -= value; }
        }

        public bool CanExecute(object parameter)
        {
            return true;
        }

        public void Execute(object parameter)
        {

            _ViewModel.SaveChanges();
        }
    }
}