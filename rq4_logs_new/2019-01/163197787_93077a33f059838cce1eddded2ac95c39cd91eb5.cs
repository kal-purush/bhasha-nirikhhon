using System;
using System.Windows.Input;

namespace SpamTool_Akhmerov.lib.MVVM
{
    public class LamdaCommand : ICommand
    {
        public event EventHandler CanExecuteChanged
        {
            add => CommandManager.RequerySuggested += value;
            remove => CommandManager.RequerySuggested -= value;
        }

        private readonly Action<object> onExecuteAction;
        private readonly Func<object, bool> canExecuteFunc;

        public LamdaCommand(Action<object> onExecuteAction, Func<object, bool> canExecuteFunc)
        {
            this.onExecuteAction = onExecuteAction;
            this.canExecuteFunc = canExecuteFunc;
        }

        public bool CanExecute(object parameter) => canExecuteFunc?.Invoke(parameter) ?? true;

        public void Execute(object parameter)
        {
            onExecuteAction?.Invoke(parameter);
        }
    }
}