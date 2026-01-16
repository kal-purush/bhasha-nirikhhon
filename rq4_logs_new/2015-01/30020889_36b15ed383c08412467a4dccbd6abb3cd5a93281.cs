using CopyPaste.ViewModels;

namespace CopyPaste.Commands
{
    using System.Windows.Input;
    using System;

    internal class ClearListCommand : ICommand
    {
        public ClearListCommand(bool canExecute, CopyPasteItemViewModel cpivm)
        {
            _canExecute = canExecute;
            _cpivm = cpivm;
        }

        private readonly bool _canExecute;
        private readonly CopyPasteItemViewModel _cpivm;

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
            _cpivm.ClearList();
        }
        #endregion
    }
}