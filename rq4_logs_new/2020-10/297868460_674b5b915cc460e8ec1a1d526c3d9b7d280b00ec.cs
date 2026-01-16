using MailSender.Infrastructure.Commands;
using MailSender.Models;
using MailSender.ViewModels.Base;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Windows.Input;

namespace MailSender.ViewModels
{
    partial class MainWindowViewModel : ViewModel
    {
        private ICommand _DeleteSenderCommand;

        public ICommand DeleteSenderCommand => _DeleteSenderCommand
            ??= new LambdaCommand(OnDeleteSenderCommandExecuted, CanDeleteSenderCommandExecute);

        private bool CanDeleteSenderCommandExecute(object p) => p is Sender || SelectSenderSettings != null;

        private void OnDeleteSenderCommandExecuted(object p)
        {
            var sender = p as Sender ?? SelectSenderSettings;
            if (sender is null) return;

            SenderCollection.Remove(sender);
            SelectSenderSettings = SenderCollection.FirstOrDefault();
        }

        private ICommand _SaveSenderCommand;

        public ICommand SaveSenderCommand => _SaveSenderCommand
            ??= new LambdaCommand(OnSaveSenderCommandExecuted, CanSaveSenderCommandExecute);

        private bool CanSaveSenderCommandExecute(object p) => true;

        private void OnSaveSenderCommandExecuted(object p)
        {
            var value = (object[])p;
            var Name = (string)value[0];
            var Address = (string)value[1];
            var Server = (string)value[2];
            var Password = (string)value[3];
            var UseSSl = (bool)value[5];
            SelectSenderSettings.Name = Name;
            SelectSenderSettings.Server = Server;
            if (Int32.TryParse((string)value[4], out int Port))
            {
                SelectSenderSettings.Port = Port;
            }            
            SelectSenderSettings.UseSSl = UseSSl;
            SelectSenderSettings.Address = Address;
            if (!(Password is null))
            {
                if (Password.Length >0)
                {
                    SelectSenderSettings.Password = Password;
                }
            }
        }

        private ICommand _AddSenderCommand;

        public ICommand AddSenderCommand => _AddSenderCommand
            ??= new LambdaCommand(OnAddSenderCommandExecuted, CanAddSenderCommandExecute);

        private bool CanAddSenderCommandExecute(object p) => true;

        private void OnAddSenderCommandExecuted(object p)
        {
            var sender = new Sender();
            SenderCollection.Add(sender);
            SelectSenderSettings = sender;
        }
    }
}