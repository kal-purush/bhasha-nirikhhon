using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class ShowEventDetailCommand : ICommand
    {
        public ShowEventDetailCommand(EventsViewModel viewModel)
        {
            _ViewModel = viewModel;
        }
        private EventsViewModel _ViewModel;

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
            Debug.Assert(false, _ViewModel.SelectedEvent.Title);
        }
    }
}