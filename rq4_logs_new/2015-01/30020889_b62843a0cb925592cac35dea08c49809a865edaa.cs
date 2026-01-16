namespace CopyPaste.Commands
{
    using System.Windows;
    using System.Windows.Input;
    using CopyPaste.ViewModels;
    using System;
    using CopyPaste.Models;
    using System.Collections.ObjectModel;

    internal class CopyItemFromListCommand : ICommand
    {
        public CopyItemFromListCommand(bool canExecute, CopyPasteItem cpi)
        {
            _cpi = cpi;
            _canExecute = canExecute;
        }

        private bool _canExecute;
        private CopyPasteItem _cpi;

        #region ICommand Members
        public event EventHandler CanExecuteChanged
        {
            add { CommandManager.RequerySuggested += value; }
            remove { CommandManager.RequerySuggested -= value; }
        }

        public bool CanExecute(object parameter)
        {
            return _canExecute;
        }

        public void Execute(object parameter)
        {
            _cpi.CopyItemFromList((string)parameter);
        }
        #endregion
    }
}