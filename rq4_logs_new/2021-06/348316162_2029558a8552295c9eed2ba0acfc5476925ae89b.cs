using EventPlanner.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace EventPlanner.Commands
{
    class CreateEditWindowsCommand : ICommand
    {
        public CreateEditWindowsCommand()
        {

        }

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
            if(parameter is Organizer){
                Modals.Admin.EditOrganizerModal window = new Modals.Admin.EditOrganizerModal();
                window.DataContext = new ViewModels.OneOrganizerViewModel((Organizer)parameter);
                window.Show();
            }else if(parameter is Collaborator)
            {
                Modals.Admin.EditCollaboratorModal window = new Modals.Admin.EditCollaboratorModal();
                window.DataContext = new ViewModels.OneCollaboratorViewModel((Collaborator)parameter);
                window.Show();
            }
            else if(parameter is User)
            {

            }
            else
            {

            }
            
        }
    }
}