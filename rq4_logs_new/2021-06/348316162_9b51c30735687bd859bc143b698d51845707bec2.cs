using EventPlanner.Models;
using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class AddTableCommand : ICommand
    {
        public AddTableCommand(SeatingPlanViewModel viewModel)
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
            return true;
        }

        public void Execute(object parameter)
        {
            _ViewModel.Tables.Add(new TableViewModel(new Table("name", new ObservableCollection<string>())));
        }
    }
}