using System;
using System.Collections.Generic;
using System.Text;

namespace Course3.Infrasrtucture.Commands
{
    class CommandOpenSendWindow : Command
    {
        public override bool CanExecute(object parameter) => true;

        public override void Execute(object parameter)
        {
            SendWindow x = new SendWindow();
            x.Show();
        }
    }
}