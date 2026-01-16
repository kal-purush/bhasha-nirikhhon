using MailSender.Infrastructure.Commands.Base;
using MailSender.Views.Windows;
using System;
using System.Windows;

namespace JabberClientWPF_MVVM.Infrastructure.Commands
{
    class SendWindowViewCommand : Command
    {
        private SendWindow _Window;

        public override bool CanExecute(object parameter) => _Window == null;


        public override void Execute(object parameter)
        {
            var window = new SendWindow
            {
                Owner = Application.Current.MainWindow
            };
            _Window = window;
            window.Closed += OnWindowClosed;

            window.ShowDialog();
        }
        private void OnWindowClosed(object Sender, EventArgs E)
        {
            ((Window)Sender).Closed -= OnWindowClosed;
            _Window = null;
        }
    }
}