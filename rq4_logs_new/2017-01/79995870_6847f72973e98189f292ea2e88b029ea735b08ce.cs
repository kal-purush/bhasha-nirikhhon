using GalaSoft.MvvmLight.CommandWpf;
using System.ComponentModel;
using System.Diagnostics;

namespace Client
{
    public class Connection
    {
        public string Login { get; set; }
        public string Password { get; set; }
        public RelayCommand TryConnectionCommand { get; private set; }

        public Connection()
        {
            TryConnectionCommand = new RelayCommand(Try, CanTry);
        }

        public void Try()
        {
            Trace.WriteLine(Login + " " + Password);
        }

        public bool CanTry()
        {
            return !string.IsNullOrWhiteSpace(Login) && !string.IsNullOrWhiteSpace(Password);
        }
    }
}