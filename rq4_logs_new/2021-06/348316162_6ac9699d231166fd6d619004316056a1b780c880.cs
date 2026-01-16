using EventPlanner.Models;
using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class CancelEditingTaskCommand : ICommand
    {
        public CancelEditingTaskCommand(TaskViewModel viewModel)
        {
            _ViewModel = viewModel;
        }
        private TaskViewModel _ViewModel;

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
            _ViewModel.Temp = new Task(_ViewModel.Task);
            if(_ViewModel.Mode == Mode.Adding)
            {
                // zatvori prozor
            } else if(_ViewModel.Mode == Mode.Editing)
            {
                _ViewModel.Mode = Mode.Viewing;
            }
        }
    }
}