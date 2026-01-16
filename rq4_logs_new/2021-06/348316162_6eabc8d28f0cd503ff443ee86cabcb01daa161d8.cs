using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class FilterOrganizersCommand : ICommand
    {
        public FilterOrganizersCommand(OrganizersViewModel viewModel)
        {
            _ViewModel = viewModel;
        }

        private OrganizersViewModel _ViewModel;

        #region ICommand Members

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
            _ViewModel.FilterOrganizers((string)parameter);
        }

        #endregion
    }
}