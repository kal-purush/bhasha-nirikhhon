using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class AddInvitationCommand : ICommand
    {
        public AddInvitationCommand(SeatingPlanViewModel viewModel)
        {
            _ViewModel = viewModel;
        }
        private SeatingPlanViewModel _ViewModel;

        public event EventHandler CanExecuteChanged
        {
            add { CommandManager.RequerySuggested += value; }
            remove { CommandManager.RequerySuggested -= value; }
        }

        public bool CanExecute(object parameter)
        {
            return _ViewModel.CanUpdate((string)parameter);
        }

        public void Execute(object parameter)
        {
            _ViewModel.AddInvitation();
        }
    }
}