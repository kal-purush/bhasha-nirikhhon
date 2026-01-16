using EventPlanner.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class OpenModalCommand : ICommand
    {
        public OpenModalCommand() { }

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
            if(parameter is Event)
            {
                var eventDetailsModal = new Modals.EventDetailsModal();
                eventDetailsModal.DataContext = new ViewModels.EventDetailsViewModel((Event)parameter, false, false);
                eventDetailsModal.Show();
            }
        }
    }
}