using System;
using System.Diagnostics;
using System.Windows.Input;

namespace SCQueryConnect.Commands
{
    public class GoToUrl : ICommand
    {
        public bool CanExecute(object parameter)
        {
            return true;
        }

        public void Execute(object parameter)
        {
            Process.Start((string)parameter);
        }

        public event EventHandler CanExecuteChanged;
    }
}