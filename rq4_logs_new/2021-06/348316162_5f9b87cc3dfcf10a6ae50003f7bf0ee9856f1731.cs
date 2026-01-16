using EventPlanner.Models;
using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class AcceptTaskCommand : ICommand
    {
        public AcceptTaskCommand(EventBoardViewModel viewModel)
        {
            _ViewModel = viewModel;
        }
        private EventBoardViewModel _ViewModel;

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
            _ViewModel.AcceptTask((Task)parameter);
        }
    }
}