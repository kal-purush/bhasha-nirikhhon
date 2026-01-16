using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using EasyRemote.Model;
using System.Net.Sockets;



namespace EasyRemote.Views
{

    public partial class HostWindow : Window, INotifyPropertyChanged
    {
        public HostWindow()
        {
            InitializeComponent();
            DataContext = this;
        }
        public event PropertyChangedEventHandler PropertyChanged;

        public IViewModel ViewModel { get; private set; }

        private string _hostPort;
        public string HostPort
        {
            get => _hostPort;
            set
            {
                if (_hostPort != value)
                {
                    _hostPort = value;
                    OnPropertyChanged(nameof(HostPort));
                }
            }
        }

        protected void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        
        private void ContinueButton_Click(object sender, RoutedEventArgs e)
        {
            if (string.IsNullOrWhiteSpace(HostPort) || !HostPort.Contains(":"))
            {
                MessageBox.Show("Veuillez entrer une adresse IP et un port au format IP:Port.");
                return;
            }

            var parts = HostPort.Split(':');
            string ip = parts[0];
            if (!int.TryParse(parts[1], out int port))
            {
                MessageBox.Show("Le port doit être un nombre entier.");
                return;
            }

            Socket clientSocket = ViewModel.ClientControler.ConfigureServer(ip, port);
            ViewModel.ClientControler.ConnectToServer(clientSocket);
        }
        private void ExitButton_Click(object sender, RoutedEventArgs e)
        {
            ViewModel.ClientControler.DisconnectToServer(null);
            this.Close();
        }
    }
}