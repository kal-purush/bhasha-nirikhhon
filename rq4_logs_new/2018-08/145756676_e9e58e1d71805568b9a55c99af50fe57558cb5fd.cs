using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;

namespace WPF_application
{
    class DelegateCommand : ICommand
    {

        Action<object> execute;
        Func<object, bool> canExecute;

        public event EventHandler CanExecuteChanged
        {
            add { CommandManager.RequerySuggested += value; }
            remove { CommandManager.RequerySuggested -= value; }
        }
        
        //Определяет метод, который определяет, может ли данная команда выполняться в ее текущем состоянии.
        public bool CanExecute(object parameter)
        {
            if (canExecute != null) return canExecute(parameter);
            return true;
        }
        
        // Определяет метод, вызываемый при вызове данной команды.
        public void Execute(object parameter)
        {
            if(execute !=null)
            execute(parameter);
        }

        public DelegateCommand(Action<object> executeAction) : this(executeAction, null)
        {

        }
        public DelegateCommand(Action<object> executeAction, Func<object, bool> canExecuteFunc)
        {
            canExecute = canExecuteFunc;
            execute = executeAction;
        } 

    }
}