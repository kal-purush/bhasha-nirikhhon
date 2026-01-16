using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    internal class SelectUsersMessageCommand : ICommand
    {
        public SelectUsersMessageCommand(UserMessagesViewModel viewModel)
        {
            _ViewModel = viewModel;
        }
        private UserMessagesViewModel _ViewModel;

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
            _ViewModel.SelectedViewModel = parameter as MessagesViewModel;
        }

        #endregion
    }
}