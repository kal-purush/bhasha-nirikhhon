using System;
using System.Windows.Input;
using TCObjectStorageClient.ViewModels;

namespace TCObjectStorageClient.Commands
{
    public abstract class CommandBase : ICommand
    {
        protected readonly MainViewModel _viewModel;

        public CommandBase(MainViewModel viewModel)
        {
            _viewModel = viewModel;
        }

        public bool CanExecute(object parameter) => true;

        public void Execute(object parameter)
        {
            ExecuteInternal();
        }

        public event EventHandler CanExecuteChanged;

        protected abstract void ExecuteInternal();
    }
}